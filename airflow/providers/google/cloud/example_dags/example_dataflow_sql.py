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
from airflow.providers.google.cloud.operators.dataflow import DataflowStartSqlJobOperator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

BQ_SQL_DATASET = os.environ.get("GCP_DATAFLOW_BQ_SQL_DATASET", "airflow_dataflow_samples")
BQ_SQL_TABLE_INPUT = os.environ.get("GCP_DATAFLOW_BQ_SQL_TABLE_INPUT", "beam_input")
BQ_SQL_TABLE_OUTPUT = os.environ.get("GCP_DATAFLOW_BQ_SQL_TABLE_OUTPUT", "beam_output")
DATAFLOW_SQL_JOB_NAME = os.environ.get("GCP_DATAFLOW_SQL_JOB_NAME", "dataflow-sql")
DATAFLOW_SQL_LOCATION = os.environ.get("GCP_DATAFLOW_SQL_LOCATION", "us-west1")


with models.DAG(
    dag_id="example_gcp_dataflow_sql",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag_sql:
    # [START howto_operator_start_sql_job]
    start_sql = DataflowStartSqlJobOperator(
        task_id="start_sql_query",
        job_name=DATAFLOW_SQL_JOB_NAME,
        query=f"""
            SELECT
                sales_region as sales_region,
                count(state_id) as count_state
            FROM
                bigquery.table.`{GCP_PROJECT_ID}`.`{BQ_SQL_DATASET}`.`{BQ_SQL_TABLE_INPUT}`
            WHERE state_id >= @state_id_min
            GROUP BY sales_region;
        """,
        options={
            "bigquery-project": GCP_PROJECT_ID,
            "bigquery-dataset": BQ_SQL_DATASET,
            "bigquery-table": BQ_SQL_TABLE_OUTPUT,
            "bigquery-write-disposition": "write-truncate",
            "parameter": "state_id_min:INT64:2",
        },
        location=DATAFLOW_SQL_LOCATION,
        do_xcom_push=True,
    )
    # [END howto_operator_start_sql_job]
