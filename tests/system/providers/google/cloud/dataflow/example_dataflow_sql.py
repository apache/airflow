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
"""
Example Airflow DAG for Google Cloud Dataflow service
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.dataflow import DataflowStartSqlJobOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_gcp_dataflow_sql"

GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "example-project")

BQ_SQL_DATASET = os.environ.get("GCP_DATAFLOW_BQ_SQL_DATASET", "airflow_dataflow_samples")
BQ_SQL_TABLE_INPUT = os.environ.get("GCP_DATAFLOW_BQ_SQL_TABLE_INPUT", "beam_input")
BQ_SQL_TABLE_OUTPUT = os.environ.get("GCP_DATAFLOW_BQ_SQL_TABLE_OUTPUT", "beam_output")
DATAFLOW_SQL_JOB_NAME = os.environ.get("GCP_DATAFLOW_SQL_JOB_NAME", "dataflow-sql")
DATAFLOW_SQL_LOCATION = os.environ.get("GCP_DATAFLOW_SQL_LOCATION", "us-west1")

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
BQ_LOCATION = "europe-north1"

with models.DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    create_dataset_with_location = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_with_location", dataset_id=DATASET_NAME, location=BQ_LOCATION
    )

    create_table_with_location = BigQueryCreateEmptyTableOperator(
        task_id="create_table_with_location",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    # [START howto_operator_start_sql_job]

    start_sql = DataflowStartSqlJobOperator(
        task_id="start_sql_query",
        job_name=DATAFLOW_SQL_JOB_NAME,
        query=f"""
            SELECT * FROM {DATASET_NAME}.test_table;
        """,
        options={
            "bigquery-project": GCP_PROJECT_ID,
            "bigquery-dataset": DATASET_NAME,
            "bigquery-table": "test_table",
        },
        location=DATAFLOW_SQL_LOCATION,
        do_xcom_push=True,
    )

    # [END howto_operator_start_sql_job]

    delete_dataset_with_location = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset_with_location",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_dataset_with_location
        >> create_table_with_location
        # TEST EXECUTION
        >> start_sql
        # TEST TEARDOWN
        >> delete_dataset_with_location
    )

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
