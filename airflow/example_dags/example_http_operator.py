# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
### Example HTTP operator and sensor
"""
import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_http_operator', default_args=default_args)

simple_get = SimpleHttpOperator(
    http_conn_id='http_default',
    task_id='get_op',
    method='GET',
    endpoint='/search?q=airflow&output=json&client=test&site=github.com',
    headers={},
    dag=dag)
