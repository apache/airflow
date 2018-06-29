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

from datetime import datetime
import json
import mock
import unittest

from airflow import configuration
from airflow.models import Pool, BaseOperator, DAG
from airflow.settings import Session
from airflow.www import app as application


class TestPoolModelView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/pool/new/?url=/admin/pool/'

    @classmethod
    def setUpClass(cls):
        super(TestPoolModelView, cls).setUpClass()
        session = Session()
        session.query(Pool).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestPoolModelView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.pool = {
            'pool': 'test-pool',
            'slots': 777,
            'description': 'test-pool-description',
        }

    def tearDown(self):
        self.session.query(Pool).delete()
        self.session.commit()
        self.session.close()
        super(TestPoolModelView, self).tearDown()

    def test_create_pool(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(Pool).count(), 1)

    def test_create_pool_with_same_name(self):
        # create test pool
        self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        # create pool with the same name
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertIn('Already exists.', response.data.decode('utf-8'))
        self.assertEqual(self.session.query(Pool).count(), 1)

    def test_create_pool_with_empty_name(self):
        self.pool['pool'] = ''
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertIn('This field is required.', response.data.decode('utf-8'))
        self.assertEqual(self.session.query(Pool).count(), 0)


class TestExtraLinks(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.ENDPOINT = '/admin/airflow/extra_links'
        self.DEFAULT_DATE = datetime(2017, 1, 1)
        self.app = application.create_app().test_client()

        class DummyTestOperator(BaseOperator):
            extra_links = ['foo-bar']

            def get_extra_links(self, ddtm, link_name):
                if link_name == 'raise_error':
                    raise ValueError('This is an error')
                if link_name == 'no_response':
                    return None
                return 'http://www.example.com/{0}/{1}/{2}'.format(self.task_id,
                                                                   link_name, ddtm)

        self.dag = DAG('dag', start_date=self.DEFAULT_DATE)
        self.task = DummyTestOperator(task_id="some_dummy_task", dag=self.dag)

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_extra_links_works(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.app.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=foo-bar"
            .format(self.ENDPOINT, self.dag.dag_id, self.task.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': ('http://www.example.com/some_dummy_task/'
                    'foo-bar/2017-01-01T00:00:00+00:00'),
            'error': None
        })

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_extra_links_error_raised(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.app.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=raise_error"
            .format(self.ENDPOINT, self.dag.dag_id, self.task.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(404, response.status_code)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': None,
            'error': 'This is an error'})

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_extra_links_no_response(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.app.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=no_response"
            .format(self.ENDPOINT, self.dag.dag_id, self.task.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 404)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': None,
            'error': 'No URL found for no_response'})


if __name__ == '__main__':
    unittest.main()
