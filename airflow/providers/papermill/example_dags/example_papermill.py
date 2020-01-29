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
This DAG will use Papermill to run the notebook "hello_world", based on the execution date
it will create an output notebook "out-<date>". All fields, including the keys in the parameters, are
templated.
"""
import os
from datetime import timedelta

import scrapbook as sb

from airflow.lineage import AUTO
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

with DAG(
    dag_id='example_papermill_operator',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
) as dag_1:
    # [START howto_operator_papermill]
    run_this = PapermillOperator(
        task_id="run_example_notebook",
        input_nb="/tmp/hello_world.ipynb",
        output_nb="/tmp/out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
    )
    # [END howto_operator_papermill]


def check_notebook(inlets, execution_date):
    """
    Verify the message in the notebook
    """
    notebook = sb.read_notebook(inlets[0].url)
    message = notebook.scraps['message']
    print(f"Message in notebook {message} for {execution_date}")

    if message.data != f"Ran from Airflow at {execution_date}!":
        return False

    return True


with DAG(
    dag_id='example_papermill_operator',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60)
) as dag_2:

    run_this = PapermillOperator(
        task_id="run_example_notebook",
        input_nb=os.path.join(os.path.dirname(os.path.realpath(__file__)),
                              "input_notebook.ipynb"),
        output_nb="/tmp/out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
    )

    check_output = PythonOperator(
        task_id='check_out',
        python_callable=check_notebook,
        inlets=AUTO
    )

    check_output.set_upstream(run_this)
