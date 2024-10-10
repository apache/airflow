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
Example Airflow DAG for Google Cloud Dataflow YAML service.

Requirements:
    This test requires ``gcloud`` command (Google Cloud SDK) to be installed on the Airflow worker
    <https://cloud.google.com/sdk/docs/install>`__
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import DataflowStartYamlJobOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataflow_yaml"
REGION = "europe-west2"
DATAFLOW_YAML_JOB_NAME = f"{DAG_ID}_{ENV_ID}".replace("_", "-")
BQ_DATASET = f"{DAG_ID}_{ENV_ID}".replace("-", "_")
BQ_INPUT_TABLE = f"input_{DAG_ID}".replace("-", "_")
BQ_OUTPUT_TABLE = f"output_{DAG_ID}".replace("-", "_")
DATAFLOW_YAML_PIPELINE_FILE_URL = (
    "gs://airflow-system-tests-resources/dataflow/yaml/example_beam_yaml_bq.yaml"
)

BQ_VARIABLES = {
    "project": PROJECT_ID,
    "dataset": BQ_DATASET,
    "input": BQ_INPUT_TABLE,
    "output": BQ_OUTPUT_TABLE,
}

BQ_VARIABLES_DEF = {
    "project": PROJECT_ID,
    "dataset": BQ_DATASET,
    "input": BQ_INPUT_TABLE,
    "output": f"{BQ_OUTPUT_TABLE}_def",
}

INSERT_ROWS_QUERY = (
    f"INSERT {BQ_DATASET}.{BQ_INPUT_TABLE} VALUES "
    "('John Doe', 900, 'USA'), "
    "('Alice Storm', 1200, 'Australia'),"
    "('Bob Max', 1000, 'Australia'),"
    "('Peter Jackson', 800, 'New Zealand'),"
    "('Hobby Doyle', 1100, 'USA'),"
    "('Terrance Phillips', 2222, 'Canada'),"
    "('Joe Schmoe', 1500, 'Canada'),"
    "('Dominique Levillaine', 2780, 'France');"
)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "dataflow", "yaml"],
) as dag:
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_DATASET,
        location=REGION,
    )

    create_bq_input_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_input_table",
        dataset_id=BQ_DATASET,
        table_id=BQ_INPUT_TABLE,
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    insert_data_into_bq_table = BigQueryInsertJobOperator(
        task_id="insert_data_into_bq_table",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=REGION,
    )

    # [START howto_operator_dataflow_start_yaml_job]
    start_dataflow_yaml_job = DataflowStartYamlJobOperator(
        task_id="start_dataflow_yaml_job",
        job_name=DATAFLOW_YAML_JOB_NAME,
        yaml_pipeline_file=DATAFLOW_YAML_PIPELINE_FILE_URL,
        append_job_name=True,
        deferrable=False,
        region=REGION,
        project_id=PROJECT_ID,
        jinja_variables=BQ_VARIABLES,
    )
    # [END howto_operator_dataflow_start_yaml_job]

    # [START howto_operator_dataflow_start_yaml_job_def]
    start_dataflow_yaml_job_def = DataflowStartYamlJobOperator(
        task_id="start_dataflow_yaml_job_def",
        job_name=DATAFLOW_YAML_JOB_NAME,
        yaml_pipeline_file=DATAFLOW_YAML_PIPELINE_FILE_URL,
        append_job_name=True,
        deferrable=True,
        region=REGION,
        project_id=PROJECT_ID,
        jinja_variables=BQ_VARIABLES_DEF,
        expected_terminal_state=DataflowJobStatus.JOB_STATE_DONE,
    )
    # [END howto_operator_dataflow_start_yaml_job_def]

    delete_bq_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_bq_dataset",
        dataset_id=BQ_DATASET,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bq_dataset
        >> create_bq_input_table
        >> insert_data_into_bq_table
        # TEST BODY
        >> [start_dataflow_yaml_job, start_dataflow_yaml_job_def]
        # TEST TEARDOWN
        >> delete_bq_dataset
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
