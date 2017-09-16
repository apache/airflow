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

from airflow import configuration
from airflow import models
from airflow.settings import Session
from airflow.www import app as application


class TestChartModelView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/chart/new/?url=/admin/chart/'

    @classmethod
    def setUpClass(cls):
        super(TestChartModelView, cls).setUpClass()
        session = Session()
        session.query(models.Chart).delete()
        session.query(models.User).delete()
        session.commit()
        user = models.User(username='airflow')
        session.add(user)
        session.commit()
        session.close()

    def setUp(self):
        super(TestChartModelView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.chart = {
            'label': 'chart',
            'owner': 'airflow',
            'conn_id': 'airflow_ci',
        }

    def tearDown(self):
        self.session.query(models.Chart).delete()
        self.session.commit()
        self.session.close()
        super(TestChartModelView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        super(TestChartModelView, cls).tearDownClass()

    def test_create_chart(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.chart,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.Chart).count(), 1)

    def test_get_chart(self):
        response = self.app.get(
            '/admin/chart?sort=3',
            follow_redirects=True,
        )
        print(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertIn('Sort by Owner', response.data.decode('utf-8'))


class TestVariableView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/variable/new/?url=/admin/variable/'

    @classmethod
    def setUpClass(cls):
        super(TestVariableView, cls).setUpClass()
        session = Session()
        session.query(models.Variable).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestVariableView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.variable = {
            'key': 'test_key',
            'val': 'text_val',
            'is_encrypted': True
        }

    def tearDown(self):
        self.session.query(models.Variable).delete()
        self.session.commit()
        self.session.close()
        super(TestVariableView, self).tearDown()

    def test_can_handle_error_on_decrypt(self):
        # create valid variable
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.variable,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)

        # update the variable with a wrong value, given that is encrypted
        Var = models.Variable
        (self.session.query(Var)
            .filter(Var.key == self.variable['key'])
            .update({
                'val': 'failed_value_not_encrypted'
            }, synchronize_session=False))
        self.session.commit()

        # retrieve Variables page, should not fail and contain the Invalid
        # label for the variable
        response = self.app.get('/admin/variable', follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.Variable).count(), 1)
        self.assertIn('<span class="label label-danger">Invalid</span>',
                      response.data.decode('utf-8'))

    def test_xss_prevention(self):
        xss = "/admin/airflow/variables/asdf<img%20src=''%20onerror='alert(1);'>"

        response = self.app.get(
            xss,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("<img src='' onerror='alert(1);'>",
                         response.data.decode("utf-8"))


class TestKnownEventView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/knownevent/new/?url=/admin/knownevent/'

    @classmethod
    def setUpClass(cls):
        super(TestKnownEventView, cls).setUpClass()
        session = Session()
        session.query(models.KnownEvent).delete()
        session.query(models.User).delete()
        session.commit()
        user = models.User(username='airflow')
        session.add(user)
        session.commit()
        cls.user_id = user.id
        session.close()

    def setUp(self):
        super(TestKnownEventView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        self.known_event = {
            'label': 'event-label',
            'event_type': '1',
            'start_date': '2017-06-05 12:00:00',
            'end_date': '2017-06-05 13:00:00',
            'reported_by': self.user_id,
            'description': '',
        }

    def tearDown(self):
        self.session.query(models.KnownEvent).delete()
        self.session.commit()
        self.session.close()
        super(TestKnownEventView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        super(TestKnownEventView, cls).tearDownClass()

    def test_create_known_event(self):
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.known_event,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.session.query(models.KnownEvent).count(), 1)

    def test_create_known_event_with_end_data_earlier_than_start_date(self):
        self.known_event['end_date'] = '2017-06-05 11:00:00'
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.known_event,
            follow_redirects=True,
        )
        self.assertIn(
            'Field must be greater than or equal to Start Date.',
            response.data.decode('utf-8'),
        )
        self.assertEqual(self.session.query(models.KnownEvent).count(), 0)


class TestPoolModelView(unittest.TestCase):

    CREATE_ENDPOINT = '/admin/pool/new/?url=/admin/pool/'

    @classmethod
    def setUpClass(cls):
        super(TestPoolModelView, cls).setUpClass()
        session = Session()
        session.query(models.Pool).delete()
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
        self.session.query(models.Pool).delete()
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
        self.assertEqual(self.session.query(models.Pool).count(), 1)

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
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_create_pool_with_empty_name(self):
        self.pool['pool'] = ''
        response = self.app.post(
            self.CREATE_ENDPOINT,
            data=self.pool,
            follow_redirects=True,
        )
        self.assertIn('This field is required.', response.data.decode('utf-8'))
        self.assertEqual(self.session.query(models.Pool).count(), 0)


if __name__ == '__main__':
    unittest.main()
