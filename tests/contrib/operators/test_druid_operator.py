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
#

import mock
import unittest

from airflow import DAG, configuration
from airflow.contrib.operators.druid_operator import DruidOperator
from airflow.utils import timezone
from airflow.models import TaskInstance

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestDruidOperator(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': timezone.datetime(2017, 1, 1)
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
            self.assertEqual(druid.index_spec_str, '{\n    "some": "json"\n}')

    def test_render_template(self):
        json_str = '''
            {
                "type": "{{ params.index_type }}",
                "datasource": "{{ params.datasource }}",
                "spec": {
                    "dataSchema": {
                        "granularitySpec": {
                            "intervals": ["{{ ds }}/{{ macros.ds_add(ds, 1) }}"]
                        }
                    }
                }
            }
        '''
        m = mock.mock_open(read_data=json_str)
        with mock.patch('airflow.contrib.operators.druid_operator.open', m, create=True) as m:
            operator = DruidOperator(
                task_id='spark_submit_job',
                json_index_file='index_spec.json',
                params={
                    'index_type': 'index_hadoop',
                    'datasource': 'datasource_prd'
                },
                dag=self.dag
            )
            ti = TaskInstance(operator, DEFAULT_DATE)
            ti.render_templates()

            m.assert_called_once_with('index_spec.json')
            expected = '''{
    "datasource": "datasource_prd",
    "spec": {
        "dataSchema": {
            "granularitySpec": {
                "intervals": [
                    "2017-01-01/2017-01-02"
                ]
            }
        }
    },
    "type": "index_hadoop"
}'''
            self.assertEqual(expected, getattr(operator, 'index_spec_str'))


if __name__ == '__main__':
    unittest.main()
