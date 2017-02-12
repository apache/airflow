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
from airflow import configuration
configuration.load_test_config()

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.dataflow import Dataflow

TEST_DAG_ID = 'test_dataflow_dag'
DEFAULT_DATE = datetime(2017, 1, 1)

class DataflowTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'end_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

        self.tuple_data = ('abc', [1, 2, 3])
        self.dict_data = {'a': 'abc', 'b': [1, 2, 3]}
        self.bash_data = 'Hello, World!'

        with self.dag:
            self.tuple_data_generator = PythonOperator(
                task_id='tuple_data_generator',
                python_callable=lambda: self.tuple_data)

            self.dict_data_generator = PythonOperator(
                task_id='dict_data_generator',
                python_callable=lambda: self.dict_data)

            self.bash_data_generator = BashOperator(
                task_id='bash_data_generator',
                bash_command=r'echo "{}"'.format(self.bash_data))

        self.dataflow_1 = Dataflow(self.tuple_data_generator, index=1)
        self.dataflow_2 = Dataflow(self.dict_data_generator, index='a')
        self.dataflow_3 = Dataflow(self.bash_data_generator)

    def check_dataflows(self, x, y, z):
        if x != self.tuple_data[1]:
            raise Exception('failure: tuple_data')
        if y != self.dict_data['a']:
            raise Exception('failure: dict_data')
        if z != self.bash_data:
            raise Exception('failure: bash_data')

    def test_create_dataflows(self):
        """
        Test that dataflow objects can be created and properly create
        task relationships
        """

        with self.dag:
            op1 = DummyOperator(
                task_id='op1',
                dataflows=dict(x=self.dataflow_1, y=self.dataflow_2))

            self.assertTrue(('x', self.dataflow_1) in op1.dataflows.items())
            self.assertTrue(('y', self.dataflow_2) in op1.dataflows.items())
            self.assertTrue(
                self.tuple_data_generator.task_id in op1.upstream_task_ids)
            self.assertTrue(
                op1.task_id in self.tuple_data_generator.downstream_task_ids)

            op2 = DummyOperator(task_id='op2')
            op2.add_dataflows(
                x=self.dataflow_1,
                y=self.dataflow_2,
                z=self.dataflow_3)
            self.assertTrue(('z', self.dataflow_3) in op2.dataflows.items())

    def test_run_dataflows(self):

        with self.dag:
            # Checks that dataflows are available in the Operator context
            def check_dataflows_context(**context):
                self.check_dataflows(
                    x=context['dataflows']['x'],
                    y=context['dataflows']['y'],
                    z=context['dataflows']['z'])
            op_context = PythonOperator(
                task_id='test_run_dataflows_context',
                python_callable=check_dataflows_context,
                provide_context=True,
                dataflows=dict(
                    x=self.dataflow_1,
                    y=self.dataflow_2,
                    z=self.dataflow_3))

            # Checks that dataflows are passed directly to the Operator as
            # keyword args
            def check_dataflows_kwargs(x, y, z):
                self.check_dataflows(x=x, y=y, z=z)
            op_kwargs = PythonOperator(
                task_id='test_run_dataflows_kwargs',
                python_callable=check_dataflows_kwargs,
                dataflows=dict(
                    x=self.dataflow_1,
                    y=self.dataflow_2,
                    z=self.dataflow_3))

            # Checks that dataflows are available in bash context
            op_bash_context = BashOperator(
                task_id='test_run_dataflows_bash',
                bash_command="""
                    if [ $({{ dataflows['z'] }}) == 'Hello, World!']; then
                        echo 'success!'
                    else
                        exit 1
                    fi
                    """,
                dataflows=dict(
                    x=self.dataflow_1,
                    y=self.dataflow_2,
                    z=self.dataflow_3))

        # run generators
        self.tuple_data_generator.run()
        self.dict_data_generator.run()
        self.bash_data_generator.run()

        # run downstream tasks
        op_context.run()
        op_kwargs.run()
        op_bash_context.run()


if __name__ == '__main__':
    unittest.main()
