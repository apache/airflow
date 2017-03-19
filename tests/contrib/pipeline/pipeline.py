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

from __future__ import print_function

import unittest
from datetime import datetime
import functools
import json
from airflow import configuration
configuration.load_test_config()

from airflow import DAG
from airflow.models import BaseOperator, XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.pipeline import add_pipelines
from airflow.contrib.pipeline.filesystem_pipeline import FileSystemPipeline

TEST_DAG_ID = 'test_pipeline_dag'
DEFAULT_DATE = datetime(2017, 1, 1)


class PipelineTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.dag = lambda: DAG(
            TEST_DAG_ID,
            default_args=dict(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE))

        self.dict_data = {'a': 'abc', 'b': [1, 2, 3]}

    def test_create_pipelines(self):
        """
        Test that pipeline objects can be created and properly create
        task relationships
        """

        with self.dag():

            upstream = PythonOperator(
                task_id='upstream', python_callable=lambda: self.dict_data)

            op1 = DummyOperator(task_id='op1')
            add_pipelines(
                downstream_task=op1,
                pipelines=dict(x=dict(task=upstream, key='a')))

            self.assertTrue(upstream.task_id in op1.upstream_task_ids)
            self.assertTrue(op1.task_id in upstream.downstream_task_ids)

    def test_serialize_object_error(self):
        """
        Tests that attempting to serialize an object raises an error
        """

        with self.dag():
            upstream = PythonOperator(
                task_id='upstream', python_callable=lambda: self.dict_data)

            op1 = DummyOperator(task_id='op1')

            # this _add_pipelines doesn't serialize as string and
            # should raise an error when the upstream task is run
            add_pipelines(
                downstream_task=op1,
                pipelines=dict(x=dict(task=upstream)),
                serialize=lambda data, context: data)

            upstream.clear()
            with self.assertRaises(TypeError):
                upstream.run()

    def test_run_pipelines(self):

        with self.dag():

            upstream = PythonOperator(
                task_id='upstream', python_callable=lambda: self.dict_data)

            # checks pipeline data was received correctly
            def check_pipelines(x, y):
                if x != self.dict_data:
                    raise ValueError('x data did not match')

                if y != self.dict_data['a']:
                    raise ValueError('y data did not match')

            # Checks that pipelines are available in the Operator context
            def check_pipelines_context(**context):
                check_pipelines(
                    context['pipeline']['x'],
                    context['pipeline']['y'],)

            # Checks that pipelines are passed directly to the Operator as
            # keyword args
            check_kwargs = PythonOperator(
                task_id='test_run_pipelines_kwargs',
                python_callable=check_pipelines)
            add_pipelines(
                downstream_task=check_kwargs,
                pipelines=dict(
                    x=dict(task=upstream),
                    y=dict(task=upstream, key='a'),))

            # Checks that pipelines are available in Operator context
            check_context = PythonOperator(
                task_id='test_run_pipelines_context',
                python_callable=check_pipelines_context,
                provide_context=True)
            add_pipelines(
                downstream_task=check_context,
                pipelines=dict(
                    x=dict(task=upstream),
                    y=dict(task=upstream, key='a'),))

            # Checks that pipelines are available in bash context
            op_bash_context = BashOperator(
                task_id='test_run_pipelines_bash',
                bash_command="""
                    if [ "{{ pipeline['x'] }}" = "abc" ]; then
                        echo 'success!'
                    else
                        exit 1
                    fi
                    """)
            add_pipelines(
                downstream_task=op_bash_context,
                pipelines=dict(x=dict(task=upstream, key='a')))

        # run generator
        upstream.clear()
        upstream.run()

        # run downstream tasks
        check_context.run()
        check_kwargs.run()
        op_bash_context.run()

    def test_filesystem_pipeline(self):

        def check_pipelines(x, y):
            if x != self.dict_data:
                raise ValueError('x data did not match')

            if y != self.dict_data['a']:
                raise ValueError('y data did not match')

        with self.dag():
            upstream = PythonOperator(
                task_id='upstream', python_callable=lambda: self.dict_data)

            op = PythonOperator(
                task_id='test_filesystem_pipeline',
                python_callable=check_pipelines)
            add_pipelines(
                pipeline_class=FileSystemPipeline,
                downstream_task=op,
                pipelines=dict(
                    x=dict(task=upstream), y=dict(task=upstream, key='a')))

        upstream.clear()
        upstream.run()

        xcoms = XCom.get_many(execution_date=DEFAULT_DATE, task_ids='upstream')

        for xcom in xcoms:
            if not xcom.key.startswith('PIPELINE'):
                continue

            self.assertTrue(
                xcom.value.startswith(
                    '/tmp/airflow/FileSystemPipeline/test_pipeline_dag/upstream'
                ))

            with open(xcom.value, 'r') as f:
                xcom_data = f.read()
            if xcom.key.endswith('a'):
                self.assertTrue(json.loads(xcom_data) == self.dict_data['a'])
            else:
                self.assertTrue(json.loads(xcom_data) == self.dict_data)

        op.run()


if __name__ == '__main__':
    unittest.main()
