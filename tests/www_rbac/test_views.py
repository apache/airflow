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

import io
import unittest

from werkzeug.test import Client

from flask_appbuilder.security.sqla.models import User as ab_user

from airflow import models, configuration
from airflow.settings import Session
from airflow.www_rbac import app as application

DEFAULT_ADMIN_USER = 'test'
DEFAULT_ADMIN_PASSWORD = 'test'


class TestBase(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.app, self.appbuilder = application.create_app(testing=True)
        self.app.config['WTF_CSRF_ENABLED'] = False
        self.client = self.app.test_client()
        self.session = Session()

    def login(self, client, username, password):
        sm_session = self.appbuilder.sm.get_session()
        self.user = sm_session.query(ab_user).first()
        if not self.user:
            role_admin = self.appbuilder.sm.find_role('Admin')
            self.appbuilder.sm.add_user(
                username='test',
                first_name='test',
                last_name='test',
                email='test@fab.org',
                role=role_admin,
                password='test')
        return self.client.post('/login/', data=dict(
            username=username,
            password=password
        ), follow_redirects=True)

    def logout(self, client):
        return self.client.get('/logout/')

    def clear_table(self, model):
        self.session.query(model).delete()
        self.session.commit()
        self.session.close()


class TestVariableView(TestBase):
    CREATE_ENDPOINT = '/variable/add'

    def setUp(self):
        super(TestVariableView, self).setUp()
        self.variable = {
            'key': 'test_key',
            'val': 'text_val',
            'is_encrypted': True
        }

    def tearDown(self):
        self.clear_table(models.Variable)
        super(TestVariableView, self).tearDown()

    def test_login_required(self):
        resp = self.client.post(self.CREATE_ENDPOINT,
                                data=self.variable,
                                follow_redirects=True)
        self.assertIn('Access is Denied', resp.data.decode('utf-8'))

    def test_can_handle_error_on_decrypt(self):
        self.login(self.client, DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASSWORD)

        # create valid variable
        resp = self.client.post(self.CREATE_ENDPOINT,
                                data=self.variable,
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        v = self.session.query(models.Variable).first()
        self.assertEqual(v.key, 'test_key')
        self.assertEqual(v.val, 'text_val')

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
        resp = self.client.get('/variable/list', follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertIn('<span class="label label-danger">Invalid</span>',
                      resp.data.decode('utf-8'))

    def test_xss_prevention(self):
        xss = "/variable/list/<img%20src=''%20onerror='alert(1);'>"

        resp = self.client.get(
            xss,
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 404)
        self.assertNotIn("<img src='' onerror='alert(1);'>",
                         resp.data.decode("utf-8"))


class TestPoolModelView(TestBase):
    CREATE_ENDPOINT = '/pool/add'

    def setUp(self):
        super(TestPoolModelView, self).setUp()
        self.pool = {
            'pool': 'test-pool',
            'slots': 777,
            'description': 'test-pool-description',
        }

    def tearDown(self):
        self.clear_table(models.Pool)
        super(TestPoolModelView, self).tearDown()

    def test_create_pool(self):
        self.login(self.client, DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASSWORD)

        resp = self.client.post(self.CREATE_ENDPOINT,
                                data=self.pool,
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_create_pool_with_same_name(self):
        self.login(self.client, DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASSWORD)

        # create test pool
        self.client.post(self.CREATE_ENDPOINT,
                         data=self.pool,
                         follow_redirects=True)
        # create pool with the same name
        resp = self.client.post(self.CREATE_ENDPOINT,
                                data=self.pool,
                                follow_redirects=True)
        self.assertIn('Already exists.', resp.data.decode('utf-8'))
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_create_pool_with_empty_name(self):
        self.login(self.client, DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASSWORD)

        self.pool['pool'] = ''
        resp = self.client.post(self.CREATE_ENDPOINT,
                                data=self.pool,
                                follow_redirects=True)
        self.assertIn('This field is required.', resp.data.decode('utf-8'))
        self.assertEqual(self.session.query(models.Pool).count(), 0)


class TestVarImportView(TestBase):
    IMPORT_ENDPOINT = '/variable/varimport'

    def setUp(self):
        super(TestVarImportView, self).setUp()

    def tearDown(self):
        self.clear_table(models.Variable)
        super(TestVarImportView, self).tearDown()

    def test_import_variables(self):
        self.assertEqual(self.session.query(models.Variable).count(), 0)
        self.login(self.client, DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASSWORD)

        content = ('{"str_key": "str_value", "int_key": 60,'
                   '"list_key": [1, 2], "dict_key": {"k_a": 2, "k_b": 3}}')
        try:
            # python 3+
            bytes_content = io.BytesIO(bytes(content, encoding='utf-8'))
        except TypeError:
            # python 2.7
            bytes_content = io.BytesIO(bytes(content))

        resp = self.client.post(self.IMPORT_ENDPOINT,
                                data={'file': (bytes_content, 'test.json')},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertIn('4 variable(s) successfully updated.', resp.data.decode('utf-8'))


class TestMountPoint(unittest.TestCase):
    def setUp(self):
        application.app = None
        super(TestMountPoint, self).setUp()
        configuration.load_test_config()
        configuration.conf.set("webserver", "base_url", "http://localhost:8080/test")
        config = dict()
        config['WTF_CSRF_METHODS'] = []
        app = application.cached_app(config=config, testing=True)
        self.client = Client(app)

    def test_mount(self):
        response, _, _ = self.client.get('/', follow_redirects=True)
        txt = b''.join(response)
        self.assertEqual(b"Apache Airflow is not at this location", txt)

        response, _, _ = self.client.get('/test/home', follow_redirects=True)
        resp_html = b''.join(response)
        self.assertIn(b"DAGs", resp_html)


if __name__ == '__main__':
    unittest.main()
