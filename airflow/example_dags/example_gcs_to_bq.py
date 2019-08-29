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
"""
Example DAG using GoogleCloudStorageToBigQueryOperator.
"""
import airflow
from airflow import models
from airflow.operators import bash_operator

from airflow.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = models.DAG(
    dag_id='example_gcs_to_bq_operator', default_args=args,
    schedule_interval=None)

create_test_dataset = bash_operator.BashOperator(
    task_id='create_airflow_test_dataset',
    bash_command='bq mk airflow_test',
    dag=dag)

# [START howto_operator_gcs_to_bq]
load_csv = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_example',
    bucket='cloud-samples-data',
    source_objects=['bigquery/us-states/us-states.csv'],
    destination_project_dataset_table='airflow_test.gcs_to_bq_table',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)
# [END howto_operator_gcs_to_bq]

delete_test_dataset = bash_operator.BashOperator(
    task_id='delete_airflow_test_dataset',
    bash_command='bq rm -rf airflow_test',
    dag=dag)

create_test_dataset >> load_csv >> delete_test_dataset
