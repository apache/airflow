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

import datetime
import unittest

from airflow import DAG, configuration
from airflow.contrib.operators.gcp_run_dlp_job import RunDlpJobOperator
from tests.compat import mock


class TestRunDlpJobOperator(unittest.TestCase):

    _config = {
        'parent': 'projects/test_project',
        'body': {}
    }

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2019, 6, 28)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_init(self):
        operator = RunDlpJobOperator(
            task_id='run_dlp_job_operator',
            dag=self.dag,
            **self._config
        )
        self.assertEqual(operator.parent, self._config['parent'])
        self.assertEqual(operator.body, self._config['body'])

    @mock.patch('airflow.contrib.operators.gcp_run_dlp_job.CloudDataLossPreventionHook')
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = RunDlpJobOperator(
            task_id='run_dlp_job_operator',
            dag=self.dag,
            **self._config
        )
        operator.execute(None)
        mock_instance.run_dlp_job.assert_called_once_with(
            parent=self._config['parent'],
            body=self._config['body']
        )


if __name__ == '__main__':
    unittest.main()
