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
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import timedelta

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(3),
}

dag = DAG(
    dag_id='perf_dag_1', default_args=args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=60))

task_1 = BashOperator(
    task_id='perf_task_1',
    bash_command='sleep 5; echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag)

for i in range(2, 5):
    task = BashOperator(
        task_id='perf_task_{}'.format(i),
        bash_command='''
            sleep 5; echo "run_id={{ run_id }} | dag_run={{ dag_run }}"
        ''',
        dag=dag)
    task.set_upstream(task_1)

if __name__ == "__main__":
    dag.cli()
