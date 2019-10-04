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
Example DAG using BigQueryToBigQueryOperator.
"""

import airflow
from airflow.gcp.operators.bigquery import BigQueryTableDeleteOperator
from airflow.models import DAG
from airflow.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

with DAG(
    dag_id='example_bigquery_to_bigquery',
    default_args=args,
    schedule_interval=None,
) as dag:
    # [START howto_operator_bq_to_bq]
    copy_table = BigQueryToBigQueryOperator(
        task_id='copy_table',
        source_project_dataset_tables='bigquery-public-data.baseball.schedules',
        destination_project_dataset_table='bigquery-public-data.test.schedules',
        write_disposition='WRITE_EMPTY',
        create_disposition='CREATE_IF_NEEDED',
        bigquery_conn_id='bigquery_default')
    # [END howto_operator_bq_to_bq]

    delete_test_dataset = BigQueryTableDeleteOperator(
        task_id='delete_test_dataset',
        deletion_dataset_table='bigquery-public-data.test.schedules')
