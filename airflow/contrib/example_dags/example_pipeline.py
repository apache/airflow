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
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.pipeline import Pipeline, add_pipelines
import logging

with DAG(dag_id='example_pipeline',
         start_date=airflow.utils.dates.days_ago(1)) as dag:

    # -----------------------------------------------------------------
    # Upstream Task
    # -----------------------------------------------------------------

    # this task returns values that will be used by downstream tasks
    upstream_task = PythonOperator(
        task_id='upstream_task',
        python_callable=(
            lambda: {
                'number': 2,
                'numbers': [1, 2, 3],
                'command': 'echo "Hello, World!"',}))

    # -----------------------------------------------------------------
    # Downstream Tasks
    # -----------------------------------------------------------------

    # this task renders upstream data into its template
    downstream_bash = BashOperator(
        task_id='downstream_bash',
        bash_command="""
            echo "here is the output of the pipelined command:"
            {{ pipeline['command']}}
            """)

    # this task consumes upstream data in a function
    def fn(all_data, some_data):
        logging.info('here is all the data: {}'.format(all_data))
        logging.info('here is just some of the some_data: {}'.format(some_data))

    downstream_python = PythonOperator(
        task_id='downstream_python', python_callable=fn)

    # -----------------------------------------------------------------
    # Pipelines
    # -----------------------------------------------------------------

    # This Pipeline is instantiated directly
    Pipeline(
        # the task that will create the data
        upstream_task=upstream_task,
        # select this key from the upstream data
        upstream_key='command',
        # the task that will consume the data
        downstream_task=downstream_bash,
        # the key the data will be available under
        downstream_key='command')

    # These Pipelines are created with a convenience function
    # the pipelines are specified as:
    #     {downstream_key: {task: upstream_task, key: upstream_key}}
    add_pipelines(
        downstream_task=downstream_python,
        pipelines={
            'all_data': {
                'task': upstream_task
            },
            'some_data': {
                'task': upstream_task,
                'key': 'numbers'
            }
        },
        pipeline_class=Pipeline)
