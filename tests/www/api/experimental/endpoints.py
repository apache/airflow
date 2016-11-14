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

import os
import unittest
from datetime import datetime, time, timedelta

from airflow import configuration

configuration.load_test_config()
from airflow import models, settings
from airflow.www import app as application
from airflow.settings import Session

NUM_EXAMPLE_DAGS = 16
DEV_NULL = '/dev/null'
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')
DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_tests'


try:
    import cPickle as pickle
except ImportError:
    # Python 3
    import pickle


def reset(dag_id=TEST_DAG_ID):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


class ApiExperimentalTests(unittest.TestCase):
    def setUp(self):
        reset()
        configuration.load_test_config()
        app = application.create_app()
        app.config['TESTING'] = True
        self.app = app.test_client()

        self.dagbag = models.DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        self.dag_bash = self.dagbag.dags['example_bash_operator']
        self.runme_0 = self.dag_bash.get_task('runme_0')

    def test_task_info(self):
        url_template = '/api/experimental/dags/{}/tasks/{}'

        response = self.app.get(url_template.format('example_bash_operator', 'runme_0'))
        assert '"email"' in response.data.decode('utf-8')
        assert 'error' not in response.data.decode('utf-8')
        self.assertEqual(200, response.status_code)
       
        response = self.app.get(url_template.format('example_bash_operator', 'DNE'))
        assert 'error' in response.data.decode('utf-8')
        self.assertEqual(404, response.status_code)

        response = self.app.get(url_template.format('DNE', 'DNE'))
        assert 'error' in response.data.decode('utf-8')
        self.assertEqual(404, response.status_code)

    def tearDown(self):
        self.dag_bash.clear(start_date=DEFAULT_DATE, end_date=datetime.now())
