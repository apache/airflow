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
from airflow.contrib.dataflow.file_dataflow import FileDataflow

TEST_DAG_ID = 'test_dataflow_dag'
DEFAULT_DATE = datetime(2017, 1, 1)

class FileDataflowTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE,
            'end_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

        self.data = {'a': 'abc', 'b': [1, 2, 3]}

        with self.dag:
            self.data_generator = PythonOperator(
                task_id='data_generator',
                python_callable=lambda: self.data)

    def test_file_dataflows(self):

        with self.dag:
            # Checks that dataflows are available in the Operator context
            def check_dataflows(pickle, json):
                if pickle != self.data['a']:
                    raise Exception('failure: pickle')
                if json != self.data:
                    raise Exception('failure: json')

            op = PythonOperator(
                task_id='test_run_file_dataflows',
                python_callable=check_dataflows,
                dataflows=dict(
                    pickle=FileDataflow(self.data_generator, index='a'),
                    json=FileDataflow(self.data_generator, serializer='json')))

        # run generator
        self.data_generator.run()

        # run downstream tasks
        op.run()


if __name__ == '__main__':
    unittest.main()
