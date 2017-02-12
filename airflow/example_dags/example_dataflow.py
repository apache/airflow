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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.dataflow import Dataflow
from datetime import datetime, timedelta
import logging

three_days_ago = datetime.combine(datetime.today() - timedelta(3),
                                  datetime.min.time())
args = {
    'owner': 'airflow',
    'start_date': three_days_ago,
}

def dataflow_fn(all_data, some_data, **context):
    logging.info('here is all the data: {}'.format(all_data))
    logging.info('here is just some of the some_data: {}'.format(some_data))
    logging.info('and here is the raw data in the context: {}'.format(
        context['dataflows']))

dataflow_cmd = """
    echo "these are the dataflows:"
    echo {{ dataflows }}

    echo "and here is the output of the dataflow command:"
    {{ dataflows['command']}}
    """

with DAG(dag_id='example_dataflow', default_args=args) as dag:

    data_generator = PythonOperator(
        task_id='data_generator',
        python_callable=lambda: ('string', [1, 2, 3], {'a': 'b'}))

    dict_generator = PythonOperator(
        task_id='dict_generator',
        python_callable=lambda: {'cmd': 'echo "HELLO, WORLD!"'})

    # Dataflows are defined as a dictionary of (key: Dataflow) pairs. The key
    # is how the data is accessed in the downstream task. The Dataflow object
    # tells Airflow how to serialize/deserialize the data and is passed the
    # source task and an optional index for that task's return.
    py_dataflow = PythonOperator(
        task_id='python_dataflow',
        python_callable=dataflow_fn,
        provide_context=True,
        dataflows=dict(
            all_data=Dataflow(data_generator),
            some_data=Dataflow(data_generator, index=1)))

    # Dataflows can also be provided after Operators are created
    bash_dataflow = BashOperator(
        task_id='bash_dataflow_task',
        bash_command=dataflow_cmd)

    bash_dataflow.add_dataflows(
        command=Dataflow(dict_generator, index='cmd'))
