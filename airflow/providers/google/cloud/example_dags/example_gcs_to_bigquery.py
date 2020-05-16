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
Example DAG using GCSToBigQueryOperator.
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'airflow_test')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'gcs_to_bq_table')

args = {
    'start_date': days_ago(2)
}

dag = models.DAG(
    dag_id='example_gcs_to_bigquery_operator', default_args=args,
    schedule_interval=None, tags=['example'])

create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset',
    dataset_id=DATASET_NAME,
    dag=dag
)

# [START howto_operator_gcs_to_bigquery]
load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery_example',
    bucket='cloud-samples-data',
    source_objects=['bigquery/us-states/us-states.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)
# [END howto_operator_gcs_to_bigquery]

delete_test_dataset = BigQueryDeleteDatasetOperator(
    task_id='delete_airflow_test_dataset',
    dataset_id=DATASET_NAME,
    delete_contents=True,
    dag=dag
)

create_test_dataset >> load_csv >> delete_test_dataset
