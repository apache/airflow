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
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def run_this_func(dag_run):
    """
    Print the payload "message" passed to the DagRun conf attribute.

    :param dag_run: The DagRun object
    :type dag_run: DagRun
    """
    print(f"Remotely received value of {dag_run.conf['message']} for key=message")


with DAG(
    dag_id="example_trigger_target_dag",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval=None,
    tags=['example'],
) as dag:
    run_this = PythonOperator(task_id="run_this", python_callable=run_this_func)

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "Here is the message: $message"',
        env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'},
    )
