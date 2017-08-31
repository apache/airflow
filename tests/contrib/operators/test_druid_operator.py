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
#

import datetime
import mock
import unittest

from airflow import DAG, configuration
from airflow.contrib.operators.druid_operator import DruidOperator


class TestDruidOperator(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_read_spec_from_file(self):
        m = mock.mock_open(read_data='{"some": "json"}')
        with mock.patch('airflow.contrib.operators.druid_operator.open', m, create=True) as m:
            druid = DruidOperator(
                task_id='druid_indexing_job',
                json_index_file='index_spec.json',
                dag=self.dag
            )

            m.assert_called_once_with('index_spec.json')
            self.assertEqual(druid.index_spec, {'some': 'json'})


if __name__ == '__main__':
    unittest.main()
