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
import unittest

from datetime import datetime, timedelta
from airflow.models import DagBag

import json


class ApiExperimentalTests(unittest.TestCase):
    def setUp(self):
        from airflow import configuration
        configuration.load_test_config()
        from airflow.www import app as application
        app = application.create_app(testing=True)
        self.app = app.test_client()

    def test_task_info(self):
        url_template = '/api/experimental/dags/{}/tasks/{}'

        response = self.app.get(url_template.format('example_bash_operator', 'runme_0'))
        self.assertIn('"email"', response.data.decode('utf-8'))
        self.assertNotIn('error', response.data.decode('utf-8'))
        self.assertEqual(200, response.status_code)

        response = self.app.get(url_template.format('example_bash_operator', 'DNE'))
        self.assertIn('error', response.data.decode('utf-8'))
        self.assertEqual(404, response.status_code)

        response = self.app.get(url_template.format('DNE', 'DNE'))
        self.assertIn('error', response.data.decode('utf-8'))
        self.assertEqual(404, response.status_code)

    def test_trigger_dag(self):
        url_template = '/api/experimental/dags/{}/dag_runs'
        response = self.app.post(
            url_template.format('example_bash_operator'),
            data=json.dumps(dict(run_id='my_run' + datetime.now().isoformat())),
            content_type="application/json"
        )

        self.assertEqual(200, response.status_code)

        response = self.app.post(
            url_template.format('does_not_exist_dag'),
            data=json.dumps(dict()),
            content_type="application/json"
        )
        self.assertEqual(404, response.status_code)

    def test_trigger_dag_for_date(self):
        url_template = '/api/experimental/dags/{}/dag_runs'
        dag_id = 'example_bash_operator'
        hour_from_now = datetime.now() + timedelta(hours=1)
        execution_date = datetime(hour_from_now.year,
                                  hour_from_now.month,
                                  hour_from_now.day,
                                  hour_from_now.hour)
        datetime_string = execution_date.isoformat()

        # Test Correct execution
        response = self.app.post(
            url_template.format(dag_id),
            data=json.dumps(dict(execution_date=datetime_string)),
            content_type="application/json"
        )
        self.assertEqual(200, response.status_code)

        dagbag = DagBag()
        dag = dagbag.get_dag(dag_id)
        dag_run = dag.get_dagrun(execution_date)
        self.assertTrue(dag_run,
                        'Dag Run not found for execution date {}'
                        .format(execution_date))

        # Test error for nonexistent dag
        response = self.app.post(
            url_template.format('does_not_exist_dag'),
            data=json.dumps(dict(execution_date=execution_date.isoformat())),
            content_type="application/json"
        )
        self.assertEqual(404, response.status_code)

        # Test error for bad datetime format
        response = self.app.post(
            url_template.format(dag_id),
            data=json.dumps(dict(execution_date='not_a_datetime')),
            content_type="application/json"
        )
        self.assertEqual(400, response.status_code)
