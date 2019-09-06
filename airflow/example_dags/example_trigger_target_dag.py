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
This example illustrates the use of the TriggerDagRunOperator. There are 2
entities at work in this scenario:
1. The Controller DAG - the DAG that conditionally executes the trigger
   (in example_trigger_controller.py)
2. The Target DAG - DAG being triggered

This example illustrates the following features :
1. A TriggerDagRunOperator that takes:
  a. A python callable that decides whether or not to trigger the Target DAG
  b. An optional params dict passed to the python callable to help in
     evaluating whether or not to trigger the Target DAG
  c. The id (name) of the Target DAG
  d. The python callable can add contextual info to the DagRun created by
     way of adding a Pickleable payload (e.g. dictionary of primitives). This
     state is then made available to the TargetDag
2. A Target DAG : c.f. example_trigger_target_dag.py
"""

import pprint

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

pp = pprint.PrettyPrinter(indent=4)

args = {
    'start_date': airflow.utils.dates.days_ago(2),
    'owner': 'Airflow',
}

dag = DAG(
    dag_id='example_trigger_target_dag',
    default_args=args,
    schedule_interval=None,
)


def run_this_func(**kwargs):
    """
    Print the payload "message" passed to the DagRun conf attribute.

    :param dict kwargs: Context
    """
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)

# You can also access the DagRun object in templates
bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the message: '
                 '{{ dag_run.conf["message"] if dag_run else "" }}" ',
    dag=dag,
)
