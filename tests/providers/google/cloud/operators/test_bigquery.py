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
from __future__ import annotations

import unittest
from unittest import mock
from unittest.mock import MagicMock

import pandas as pd
import pytest
from google.cloud.bigquery import DEFAULT_RETRY
from google.cloud.exceptions import Conflict

from airflow.exceptions import AirflowException, AirflowTaskTimeout, TaskDeferred
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryColumnCheckOperator,
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
from airflow.providers.google.cloud.triggers.bigquery import (
    BigQueryCheckTrigger,
    BigQueryGetDataTrigger,
    BigQueryInsertJobTrigger,
    BigQueryIntervalCheckTrigger,
    BigQueryValueCheckTrigger,
)
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags, clear_db_xcom

TASK_ID = "test-bq-generic-operator"
TEST_DATASET = "test-dataset"
TEST_DATASET_LOCATION = "EU"
TEST_GCP_PROJECT_ID = "test-project"
TEST_DELETE_CONTENTS = True
TEST_TABLE_ID = "test-table-id"
TEST_GCS_BUCKET = "test-bucket"
TEST_GCS_DATA = ["dir1/*.csv"]
TEST_SOURCE_FORMAT = "CSV"
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "test-bigquery-operators"
TEST_TABLE_RESOURCES = {"tableReference": {"tableId": TEST_TABLE_ID}, "expirationTime": 1234567}
VIEW_DEFINITION = {
    "query": f"SELECT * FROM `{TEST_DATASET}.{TEST_TABLE_ID}`",
    "useLegacySql": False,
}
MATERIALIZED_VIEW_DEFINITION = {
    "query": f"SELECT product, SUM(amount) FROM `{TEST_DATASET}.{TEST_TABLE_ID}` GROUP BY product",
    "enableRefresh": True,
    "refreshIntervalMs": 2000000,
}
TEST_TABLE = "test-table"


class TestBigQueryCreateEmptyTableOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        operator = BigQueryCreateExternalTableOperator(
            task_id=TASK_ID,
            destination_project_dataset_table=f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}",
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
                        f"gs://{TEST_GCS_BUCKET}/{source_object}" for source_object in TEST_GCS_DATA
                    ],
                    "source_format": TEST_SOURCE_FORMAT,
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        operator = BigQueryGetDatasetOperator(
            task_id=TASK_ID, dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.get_dataset.assert_called_once_with(
            dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID
        )


class TestBigQueryUpdateTableOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        table_resource = {"friendlyName": "Test TB"}
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):

        schema_field_updates = [
            {
                "name": "emp_name",
                "description": "Name of employee",
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        dataset_resource = {"friendlyName": "Test DS"}
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        dataset_resource = {"friendlyName": "Test DS"}
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

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        encryption_configuration = {"key": "kk"}

        operator = BigQueryExecuteQueryOperator(
            task_id=TASK_ID,
            sql="Select * from test_table",
            destination_dataset_table=None,
            write_disposition="WRITE_EMPTY",
            allow_large_results=False,
            flatten_results=None,
            gcp_conn_id="google_cloud_default",
            udf_config=None,
            use_legacy_sql=True,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=(),
            query_params=None,
            labels=None,
            priority="INTERACTIVE",
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
            encryption_configuration=encryption_configuration,
        )

        operator.execute(MagicMock())
        mock_hook.return_value.run_query.assert_called_once_with(
            sql="Select * from test_table",
            destination_dataset_table=None,
            write_disposition="WRITE_EMPTY",
            allow_large_results=False,
            flatten_results=None,
            udf_config=None,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=(),
            query_params=None,
            labels=None,
            priority="INTERACTIVE",
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
            encryption_configuration=encryption_configuration,
        )

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute_list(self, mock_hook):
        operator = BigQueryExecuteQueryOperator(
            task_id=TASK_ID,
            sql=[
                "Select * from test_table",
                "Select * from other_test_table",
            ],
            destination_dataset_table=None,
            write_disposition="WRITE_EMPTY",
            allow_large_results=False,
            flatten_results=None,
            gcp_conn_id="google_cloud_default",
            udf_config=None,
            use_legacy_sql=True,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=(),
            query_params=None,
            labels=None,
            priority="INTERACTIVE",
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
            encryption_configuration=None,
        )

        operator.execute(MagicMock())
        mock_hook.return_value.run_query.assert_has_calls(
            [
                mock.call(
                    sql="Select * from test_table",
                    destination_dataset_table=None,
                    write_disposition="WRITE_EMPTY",
                    allow_large_results=False,
                    flatten_results=None,
                    udf_config=None,
                    maximum_billing_tier=None,
                    maximum_bytes_billed=None,
                    create_disposition="CREATE_IF_NEEDED",
                    schema_update_options=(),
                    query_params=None,
                    labels=None,
                    priority="INTERACTIVE",
                    time_partitioning=None,
                    api_resource_configs=None,
                    cluster_fields=None,
                    encryption_configuration=None,
                ),
                mock.call(
                    sql="Select * from other_test_table",
                    destination_dataset_table=None,
                    write_disposition="WRITE_EMPTY",
                    allow_large_results=False,
                    flatten_results=None,
                    udf_config=None,
                    maximum_billing_tier=None,
                    maximum_bytes_billed=None,
                    create_disposition="CREATE_IF_NEEDED",
                    schema_update_options=(),
                    query_params=None,
                    labels=None,
                    priority="INTERACTIVE",
                    time_partitioning=None,
                    api_resource_configs=None,
                    cluster_fields=None,
                    encryption_configuration=None,
                ),
            ]
        )

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute_bad_type(self, mock_hook):
        operator = BigQueryExecuteQueryOperator(
            task_id=TASK_ID,
            sql=1,
            destination_dataset_table=None,
            write_disposition="WRITE_EMPTY",
            allow_large_results=False,
            flatten_results=None,
            gcp_conn_id="google_cloud_default",
            udf_config=None,
            use_legacy_sql=True,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=(),
            query_params=None,
            labels=None,
            priority="INTERACTIVE",
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
        )

        with pytest.raises(AirflowException):
            operator.execute(MagicMock())

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_bigquery_operator_defaults(self, mock_hook, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            task_id=TASK_ID,
            sql="Select * from test_table",
            schema_update_options=None,
        )
        operator = ti.task

        operator.execute(MagicMock())
        mock_hook.return_value.run_query.assert_called_once_with(
            sql="Select * from test_table",
            destination_dataset_table=None,
            write_disposition="WRITE_EMPTY",
            allow_large_results=False,
            flatten_results=None,
            udf_config=None,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition="CREATE_IF_NEEDED",
            schema_update_options=None,
            query_params=None,
            labels=None,
            priority="INTERACTIVE",
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
            sql="SELECT * FROM test_table",
        )
        serialized_dag = dag_maker.get_serialized_data()
        assert "sql" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        assert getattr(simple_task, "sql") == "SELECT * FROM test_table"

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################

        # Check Serialized version of operator link
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {"airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink": {}}
        ]

        # Check DeSerialized version of operator link
        assert isinstance(list(simple_task.operator_extra_links)[0], BigQueryConsoleLink)

        ti.xcom_push("job_id", 12345)

        url = simple_task.get_extra_links(ti, BigQueryConsoleLink.name)
        assert url == "https://console.cloud.google.com/bigquery?j=12345"

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
            sql=["SELECT * FROM test_table", "SELECT * FROM test_table2"],
        )
        serialized_dag = dag_maker.get_serialized_data()
        assert "sql" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        assert getattr(simple_task, "sql") == ["SELECT * FROM test_table", "SELECT * FROM test_table2"]

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################

        # Check Serialized version of operator link
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {"airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink": {"index": 0}},
            {"airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink": {"index": 1}},
        ]

        # Check DeSerialized version of operator link
        assert isinstance(list(simple_task.operator_extra_links)[0], BigQueryConsoleIndexableLink)

        job_id = ["123", "45"]
        ti.xcom_push(key="job_id", value=job_id)

        assert {"BigQuery Console #1", "BigQuery Console #2"} == simple_task.operator_extra_link_dict.keys()

        assert "https://console.cloud.google.com/bigquery?j=123" == simple_task.get_extra_links(
            ti, "BigQuery Console #1"
        )

        assert "https://console.cloud.google.com/bigquery?j=45" == simple_task.get_extra_links(
            ti, "BigQuery Console #2"
        )

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_bigquery_operator_extra_link_when_missing_job_id(
        self, mock_hook, create_task_instance_of_operator
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            task_id=TASK_ID,
            sql="SELECT * FROM test_table",
        )
        bigquery_task = ti.task

        assert "" == bigquery_task.get_extra_links(ti, BigQueryConsoleLink.name)

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_bigquery_operator_extra_link_when_single_query(
        self, mock_hook, create_task_instance_of_operator
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            sql="SELECT * FROM test_table",
        )
        bigquery_task = ti.task

        job_id = "12345"
        ti.xcom_push(key="job_id", value=job_id)

        assert f"https://console.cloud.google.com/bigquery?j={job_id}" == bigquery_task.get_extra_links(
            ti, BigQueryConsoleLink.name
        )

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_bigquery_operator_extra_link_when_multiple_query(
        self, mock_hook, create_task_instance_of_operator
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            sql=["SELECT * FROM test_table", "SELECT * FROM test_table2"],
        )
        bigquery_task = ti.task

        job_id = ["123", "45"]
        ti.xcom_push(key="job_id", value=job_id)

        assert {"BigQuery Console #1", "BigQuery Console #2"} == bigquery_task.operator_extra_link_dict.keys()

        assert "https://console.cloud.google.com/bigquery?j=123" == bigquery_task.get_extra_links(
            ti, "BigQuery Console #1"
        )

        assert "https://console.cloud.google.com/bigquery?j=45" == bigquery_task.get_extra_links(
            ti, "BigQuery Console #2"
        )


class TestBigQueryGetDataOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):

        max_results = 100
        selected_fields = "DATE"
        operator = BigQueryGetDataOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
            max_results=max_results,
            selected_fields=selected_fields,
            location=TEST_DATASET_LOCATION,
        )
        operator.execute(None)
        mock_hook.return_value.list_rows.assert_called_once_with(
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
            max_results=max_results,
            selected_fields=selected_fields,
            location=TEST_DATASET_LOCATION,
        )


class TestBigQueryTableDeleteOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_execute(self, mock_hook):
        ignore_if_missing = True
        deletion_dataset_table = f"{TEST_DATASET}.{TEST_TABLE_ID}"

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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
        (BigQueryCheckOperator, dict(sql="Select * from test_table")),
        (BigQueryValueCheckOperator, dict(sql="Select * from test_table", pass_value=95)),
        (BigQueryIntervalCheckOperator, dict(table=TEST_TABLE_ID, metrics_thresholds={"COUNT(*)": 1.5})),
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
        operator = operator_class(task_id=TASK_ID, gcp_conn_id="google_cloud_default", **kwargs)
        operator.get_db_hook()
        mock_get_db_hook.assert_called_once()


class TestBigQueryUpsertTableOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
            nowait=True,
            project_id=TEST_GCP_PROJECT_ID,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

        assert result == real_job_id

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
            nowait=True,
            project_id=TEST_GCP_PROJECT_ID,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

        assert result == real_job_id

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryJob")
    def test_on_kill_after_execution_timeout(self, mock_job, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }

        mock_job.job_id = real_job_id
        mock_job.error_result = False
        mock_job.result.side_effect = AirflowTaskTimeout()

        mock_hook.return_value.insert_job.return_value = mock_job
        mock_hook.return_value.generate_job_id.return_value = real_job_id

        op = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
            cancel_on_kill=True,
        )
        with pytest.raises(AirflowTaskTimeout):
            op.execute(context=MagicMock())

        op.on_kill()
        mock_hook.return_value.cancel_job.assert_called_once_with(
            job_id=real_job_id,
            location=TEST_DATASET_LOCATION,
            project_id=TEST_GCP_PROJECT_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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
            nowait=True,
            project_id=TEST_GCP_PROJECT_ID,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

        assert result == real_job_id

    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
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

        mock_hook.return_value.insert_job.side_effect = Conflict("any")
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


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_bigquery_insert_job_operator_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryInsertJobTrigger will be fired
    when the BigQueryInsertJobOperator is executed with deferrable=True.
    """
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

    op = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        deferrable=True,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(create_context(op))

    assert isinstance(
        exc.value.trigger, BigQueryInsertJobTrigger
    ), "Trigger is not a BigQueryInsertJobTrigger"


def test_bigquery_insert_job_operator_execute_failure():
    """Tests that an AirflowException is raised in case of error event"""
    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }
    job_id = "123456"

    operator = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        deferrable=True,
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def create_context(task):
    dag = DAG(dag_id="dag")
    logical_date = datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=logical_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "logical_date": logical_date,
    }


def test_bigquery_insert_job_operator_execute_complete():
    """Asserts that logging occurs as expected"""
    configuration = {
        "query": {
            "query": "SELECT * FROM any",
            "useLegacySql": False,
        }
    }
    job_id = "123456"

    operator = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        deferrable=True,
    )
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(
            context=create_context(operator),
            event={"status": "success", "message": "Job completed", "job_id": job_id},
        )
    mock_log_info.assert_called_with("%s completed with response %s ", "insert_query_job", "Job completed")


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_bigquery_insert_job_operator_with_job_id_generate(mock_hook):
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

    op = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        reattach_states={"PENDING"},
        deferrable=True,
    )

    with pytest.raises(TaskDeferred):
        op.execute(create_context(op))

    mock_hook.return_value.generate_job_id.assert_called_once_with(
        job_id=job_id,
        dag_id="adhoc_airflow",
        task_id="insert_query_job",
        logical_date=datetime(2022, 1, 1, 0, 0),
        configuration=configuration,
        force_rerun=True,
    )


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_execute_reattach(mock_hook):
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_hook.return_value.generate_job_id.return_value = f"{job_id}_{hash_}"

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

    op = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        reattach_states={"PENDING"},
        deferrable=True,
    )

    with pytest.raises(TaskDeferred):
        op.execute(create_context(op))

    mock_hook.return_value.get_job.assert_called_once_with(
        location=TEST_DATASET_LOCATION,
        job_id=real_job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )

    job._begin.assert_called_once_with()


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_execute_force_rerun_async(mock_hook):
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_hook.return_value.generate_job_id.return_value = f"{job_id}_{hash_}"

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
        state="DONE",
        done=lambda: False,
    )
    mock_hook.return_value.get_job.return_value = job

    op = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
        reattach_states={"PENDING"},
        deferrable=True,
    )

    with pytest.raises(AirflowException) as exc:
        op.execute(create_context(op))

    expected_exception_msg = (
        f"Job with id: {real_job_id} already exists and is in {job.state} state. "
        f"If you want to force rerun it consider setting `force_rerun=True`."
        f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
    )

    assert str(exc.value) == expected_exception_msg

    mock_hook.return_value.get_job.assert_called_once_with(
        location=TEST_DATASET_LOCATION,
        job_id=real_job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_bigquery_check_operator_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryCheckTrigger will be fired
    when the BigQueryCheckOperator is executed with deferrable=True.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryCheckOperator(
        task_id="bq_check_operator_job",
        sql="SELECT * FROM any",
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(create_context(op))

    assert isinstance(exc.value.trigger, BigQueryCheckTrigger), "Trigger is not a BigQueryCheckTrigger"


def test_bigquery_check_operator_execute_failure():
    """Tests that an AirflowException is raised in case of error event"""

    operator = BigQueryCheckOperator(
        task_id="bq_check_operator_execute_failure",
        sql="SELECT * FROM any",
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_bigquery_check_op_execute_complete_with_no_records():
    """Asserts that exception is raised with correct expected exception message"""

    operator = BigQueryCheckOperator(
        task_id="bq_check_operator_execute_complete",
        sql="SELECT * FROM any",
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    with pytest.raises(AirflowException) as exc:
        operator.execute_complete(context=None, event={"status": "success", "records": None})

    expected_exception_msg = "The query returned empty results"

    assert str(exc.value) == expected_exception_msg


def test_bigquery_check_op_execute_complete_with_non_boolean_records():
    """Executing a sql which returns a non-boolean value should raise exception"""

    test_sql = "SELECT * FROM any"

    operator = BigQueryCheckOperator(
        task_id="bq_check_operator_execute_complete",
        sql=test_sql,
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    expected_exception_msg = f"Test failed.\nQuery:\n{test_sql}\nResults:\n{[20, False]!s}"

    with pytest.raises(AirflowException) as exc:
        operator.execute_complete(context=None, event={"status": "success", "records": [20, False]})

    assert str(exc.value) == expected_exception_msg


def test_bigquery_check_operator_execute_complete():
    """Asserts that logging occurs as expected"""

    operator = BigQueryCheckOperator(
        task_id="bq_check_operator_execute_complete",
        sql="SELECT * FROM any",
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event={"status": "success", "records": [20]})
    mock_log_info.assert_called_with("Success.")


def test_bigquery_interval_check_operator_execute_complete():
    """Asserts that logging occurs as expected"""

    operator = BigQueryIntervalCheckOperator(
        task_id="bq_interval_check_operator_execute_complete",
        table="test_table",
        metrics_thresholds={"COUNT(*)": 1.5},
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
    mock_log_info.assert_called_with(
        "%s completed with response %s ", "bq_interval_check_operator_execute_complete", "Job completed"
    )


def test_bigquery_interval_check_operator_execute_failure():
    """Tests that an AirflowException is raised in case of error event"""

    operator = BigQueryIntervalCheckOperator(
        task_id="bq_interval_check_operator_execute_complete",
        table="test_table",
        metrics_thresholds={"COUNT(*)": 1.5},
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_bigquery_interval_check_operator_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryIntervalCheckTrigger will be fired
    when the BigQueryIntervalCheckOperator is executed with deferrable=True.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryIntervalCheckOperator(
        task_id="bq_interval_check_operator_execute_complete",
        table="test_table",
        metrics_thresholds={"COUNT(*)": 1.5},
        location=TEST_DATASET_LOCATION,
        deferrable=True,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(create_context(op))

    assert isinstance(
        exc.value.trigger, BigQueryIntervalCheckTrigger
    ), "Trigger is not a BigQueryIntervalCheckTrigger"


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_bigquery_get_data_operator_async_with_selected_fields(mock_hook):
    """
    Asserts that a task is deferred and a BigQuerygetDataTrigger will be fired
    when the BigQueryGetDataOperator is executed with deferrable=True.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryGetDataOperator(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id=TEST_TABLE_ID,
        max_results=100,
        selected_fields="value,name",
        deferrable=True,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(create_context(op))

    assert isinstance(exc.value.trigger, BigQueryGetDataTrigger), "Trigger is not a BigQueryGetDataTrigger"


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_bigquery_get_data_operator_async_without_selected_fields(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryGetDataTrigger will be fired
    when the BigQueryGetDataOperator is executed with deferrable=True.
    """
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"

    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)

    op = BigQueryGetDataOperator(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id=TEST_TABLE_ID,
        max_results=100,
        deferrable=True,
    )

    with pytest.raises(TaskDeferred) as exc:
        op.execute(create_context(op))

    assert isinstance(exc.value.trigger, BigQueryGetDataTrigger), "Trigger is not a BigQueryGetDataTrigger"


def test_bigquery_get_data_operator_execute_failure():
    """Tests that an AirflowException is raised in case of error event"""

    operator = BigQueryGetDataOperator(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id="any",
        max_results=100,
        deferrable=True,
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_bigquery_get_data_op_execute_complete_with_records():
    """Asserts that exception is raised with correct expected exception message"""

    operator = BigQueryGetDataOperator(
        task_id="get_data_from_bq",
        dataset_id=TEST_DATASET,
        table_id="any",
        max_results=100,
        deferrable=True,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event={"status": "success", "records": [20]})
    mock_log_info.assert_called_with("Total extracted rows: %s", 1)


def _get_value_check_async_operator(use_legacy_sql: bool = False):
    query = "SELECT COUNT(*) FROM Any"
    pass_val = 2

    return BigQueryValueCheckOperator(
        task_id="check_value",
        sql=query,
        pass_value=pass_val,
        use_legacy_sql=use_legacy_sql,
        deferrable=True,
    )


@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
def test_bigquery_value_check_async(mock_hook):
    """
    Asserts that a task is deferred and a BigQueryValueCheckTrigger will be fired
    when the BigQueryValueCheckOperatorAsync is executed.
    """
    operator = _get_value_check_async_operator(True)
    job_id = "123456"
    hash_ = "hash"
    real_job_id = f"{job_id}_{hash_}"
    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)
    with pytest.raises(TaskDeferred) as exc:
        operator.execute(create_context(operator))

    assert isinstance(
        exc.value.trigger, BigQueryValueCheckTrigger
    ), "Trigger is not a BigQueryValueCheckTrigger"


def test_bigquery_value_check_operator_execute_complete_success():
    """Tests response message in case of success event"""
    operator = _get_value_check_async_operator()

    assert (
        operator.execute_complete(context=None, event={"status": "success", "message": "Job completed!"})
        is None
    )


def test_bigquery_value_check_operator_execute_complete_failure():
    """Tests that an AirflowException is raised in case of error event"""
    operator = _get_value_check_async_operator()

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"sql": "SELECT COUNT(*) from Any"}, "missing keyword argument 'pass_value'"),
        ({"pass_value": "Any"}, "missing keyword argument 'sql'"),
    ],
)
def test_bigquery_value_check_missing_param(kwargs, expected):
    """Assert the exception if require param not pass to BigQueryValueCheckOperatorAsync operator"""
    with pytest.raises(AirflowException) as missing_param:
        BigQueryValueCheckOperator(deferrable=True, **kwargs)
    assert missing_param.value.args[0] == expected


def test_bigquery_value_check_empty():
    """Assert the exception if require param not pass to BigQueryValueCheckOperatorAsync operator"""
    expected, expected1 = (
        "missing keyword arguments 'sql', 'pass_value'",
        "missing keyword arguments 'pass_value', 'sql'",
    )
    with pytest.raises(AirflowException) as missing_param:
        BigQueryValueCheckOperator(deferrable=True, kwargs={})
    assert (missing_param.value.args[0] == expected) or (missing_param.value.args[0] == expected1)


@pytest.mark.parametrize(
    "check_type, check_value, check_result",
    [
        ("equal_to", 0, 0),
        ("greater_than", 0, 1),
        ("less_than", 0, -1),
        ("geq_to", 0, 1),
        ("geq_to", 0, 0),
        ("leq_to", 0, 0),
        ("leq_to", 0, -1),
    ],
)
@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
@mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryJob")
def test_bigquery_column_check_operator_succeeds(mock_job, mock_hook, check_type, check_value, check_result):
    mock_job.result.return_value.to_dataframe.return_value = pd.DataFrame(
        {"col_name": ["col1"], "check_type": ["min"], "check_result": [check_result]}
    )
    mock_hook.return_value.insert_job.return_value = mock_job

    op = BigQueryColumnCheckOperator(
        task_id="check_column_succeeds",
        table=TEST_TABLE_ID,
        use_legacy_sql=False,
        column_mapping={
            "col1": {"min": {check_type: check_value}},
        },
    )
    op.execute(create_context(op))


@pytest.mark.parametrize(
    "check_type, check_value, check_result",
    [
        ("equal_to", 0, 1),
        ("greater_than", 0, -1),
        ("less_than", 0, 1),
        ("geq_to", 0, -1),
        ("leq_to", 0, 1),
    ],
)
@mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
@mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryJob")
def test_bigquery_column_check_operator_fails(mock_job, mock_hook, check_type, check_value, check_result):
    mock_job.result.return_value.to_dataframe.return_value = pd.DataFrame(
        {"col_name": ["col1"], "check_type": ["min"], "check_result": [1]}
    )
    mock_hook.return_value.insert_job.return_value = mock_job

    op = BigQueryColumnCheckOperator(
        task_id="check_column_fails",
        table=TEST_TABLE_ID,
        use_legacy_sql=False,
        column_mapping={
            "col1": {"min": {"equal_to": 0}},
        },
    )
    with pytest.raises(AirflowException):
        op.execute(create_context(op))
