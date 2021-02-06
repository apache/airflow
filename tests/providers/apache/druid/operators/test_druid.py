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

import unittest

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.providers.apache.druid.operators.druid import DruidOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestDruidOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': timezone.datetime(2017, 1, 1)}
        self.dag = DAG('test_dag_id', default_args=args)

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
        operator = DruidOperator(
            task_id='spark_submit_job',
            json_index_file=json_str,
            params={'index_type': 'index_hadoop', 'datasource': 'datasource_prd'},
            dag=self.dag,
        )
        ti = TaskInstance(operator, DEFAULT_DATE)
        ti.render_templates()
        expected = '''
            {
                "type": "index_hadoop",
                "datasource": "datasource_prd",
                "spec": {
                    "dataSchema": {
                        "granularitySpec": {
                            "intervals": ["2017-01-01/2017-01-02"]
                        }
                    }
                }
            }
        '''
        assert expected == getattr(operator, 'json_index_file')
