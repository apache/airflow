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

import io
import unittest
import urllib
from werkzeug.test import Client
from flask._compat import PY2
from flask_appbuilder.security.sqla.models import User as ab_user
from airflow import models
from airflow import configuration as conf
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.www_rbac import app as application


class TestBase(unittest.TestCase):
    def setUp(self):
        conf.load_test_config()
        self.app, self.appbuilder = application.create_app(testing=True)
        self.app.config['WTF_CSRF_ENABLED'] = False
        self.client = self.app.test_client()
        self.session = Session()
        self.login()

    def login(self):
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
            username='test',
            password='test'
        ), follow_redirects=True)

    def logout(self):
        return self.client.get('/logout/')

    def clear_table(self, model):
        self.session.query(model).delete()
        self.session.commit()
        self.session.close()

    def check_content_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        self.assertEqual(resp_code, resp.status_code)
        if isinstance(text, list):
            for kw in text:
                self.assertIn(kw, resp_html)
        else:
            self.assertIn(text, resp_html)

    def percent_encode(self, obj):
        if PY2:
            return urllib.quote_plus(str(obj))
        else:
            return urllib.parse.quote_plus(str(obj))


class TestConnectionModelView(TestBase):
    def setUp(self):
        super(TestConnectionModelView, self).setUp()
        self.connection = {
            'conn_id': 'test_conn',
            'conn_type': 'http',
            'host': 'localhost',
            'port': 8080,
            'username': 'root',
            'password': 'admin'
        }

    def tearDown(self):
        self.clear_table(models.Connection)
        super(TestConnectionModelView, self).tearDown()

    def test_create_connection(self):
        resp = self.client.post('/connection/add',
                                data=self.connection,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)


class TestVariableModelView(TestBase):
    def setUp(self):
        super(TestVariableModelView, self).setUp()
        self.variable = {
            'key': 'test_key',
            'val': 'text_val',
            'is_encrypted': True
        }

    def tearDown(self):
        self.clear_table(models.Variable)
        super(TestVariableModelView, self).tearDown()

    def test_can_handle_error_on_decrypt(self):

        # create valid variable
        resp = self.client.post('/variable/add',
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
        self.check_content_in_response(
            '<span class="label label-danger">Invalid</span>', resp)

    def test_xss_prevention(self):
        xss = "/variable/list/<img%20src=''%20onerror='alert(1);'>"

        resp = self.client.get(
            xss,
            follow_redirects=True,
        )
        self.assertEqual(resp.status_code, 404)
        self.assertNotIn("<img src='' onerror='alert(1);'>",
                         resp.data.decode("utf-8"))

    def test_import_variables(self):
        self.assertEqual(self.session.query(models.Variable).count(), 0)

        content = ('{"str_key": "str_value", "int_key": 60,'
                   '"list_key": [1, 2], "dict_key": {"k_a": 2, "k_b": 3}}')
        try:
            # python 3+
            bytes_content = io.BytesIO(bytes(content, encoding='utf-8'))
        except TypeError:
            # python 2.7
            bytes_content = io.BytesIO(bytes(content))

        resp = self.client.post('/variable/varimport',
                                data={'file': (bytes_content, 'test.json')},
                                follow_redirects=True)
        self.check_content_in_response('4 variable(s) successfully updated.', resp)


class TestPoolModelView(TestBase):
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

    def test_create_pool_with_same_name(self):
        # create test pool
        resp = self.client.post('/pool/add',
                                data=self.pool,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)

        # create pool with the same name
        resp = self.client.post('/pool/add',
                                data=self.pool,
                                follow_redirects=True)
        self.check_content_in_response('Already exists.', resp)

    def test_create_pool_with_empty_name(self):

        self.pool['pool'] = ''
        resp = self.client.post('/pool/add',
                                data=self.pool,
                                follow_redirects=True)
        self.check_content_in_response('This field is required.', resp)


class TestMountPoint(unittest.TestCase):
    def setUp(self):
        application.app = None
        super(TestMountPoint, self).setUp()
        conf.load_test_config()
        conf.set("webserver", "base_url", "http://localhost:8080/test")
        config = dict()
        config['WTF_CSRF_METHODS'] = []
        app = application.cached_app(config=config, testing=True)
        self.client = Client(app)

    def test_mount(self):
        resp, _, _ = self.client.get('/', follow_redirects=True)
        txt = b''.join(resp)
        self.assertEqual(b"Apache Airflow is not at this location", txt)

        resp, _, _ = self.client.get('/test/home', follow_redirects=True)
        resp_html = b''.join(resp)
        self.assertIn(b"DAGs", resp_html)


class TestAirflowBaseViews(TestBase):
    default_date = timezone.datetime(2018, 3, 1)
    run_id = "test_{}".format(models.DagRun.id_for_date(default_date))

    def setUp(self):
        super(TestAirflowBaseViews, self).setUp()
        self.cleanup_dagruns()
        self.prepare_dagruns()

    def cleanup_dagruns(self):
        DR = models.DagRun
        dag_ids = ['example_bash_operator',
                   'example_subdag_operator',
                   'example_xcom']
        (self.session
             .query(DR)
             .filter(DR.dag_id.in_(dag_ids))
             .filter(DR.run_id == self.run_id)
             .delete(synchronize_session='fetch'))
        self.session.commit()

    def prepare_dagruns(self):
        dagbag = models.DagBag(include_examples=True)
        self.bash_dag = dagbag.dags['example_bash_operator']
        self.sub_dag = dagbag.dags['example_subdag_operator']
        self.xcom_dag = dagbag.dags['example_xcom']

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.xcom_dagrun = self.xcom_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

    def test_index(self):
        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('DAGs', resp)

    def test_health(self):
        resp = self.client.get('health')
        self.check_content_in_response('The server is healthy!', resp)

    def test_home(self):
        resp = self.client.get('home', follow_redirects=True)
        self.check_content_in_response('DAGs', resp)

    def test_task(self):
        url = ('task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_xcom(self):
        url = ('xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_rendered(self):
        url = ('rendered?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_pickle_info(self):
        url = 'pickle_info?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)

    def test_blocked(self):
        url = 'blocked'
        resp = self.client.get(url, follow_redirects=True)
        self.assertEqual(200, resp.status_code)

    def test_dag_stats(self):
        resp = self.client.get('dag_stats', follow_redirects=True)
        self.assertEqual(resp.status_code, 200)

    def test_task_stats(self):
        resp = self.client.get('task_stats', follow_redirects=True)
        self.assertEqual(resp.status_code, 200)

    def test_dag_details(self):
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG details', resp)

    def test_graph(self):
        url = 'graph?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_tree(self):
        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_duration(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_tries(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_landing_times(self):
        url = 'landing_times?days=30&dag_id=test_example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_gantt(self):
        url = 'gantt?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_code(self):
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_paused(self):
        url = 'paused?dag_id=example_bash_operator&is_paused=false'
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('OK', resp)

    def test_success(self):

        url = ('success?task_id=run_this_last&dag_id=example_bash_operator&'
               'execution_date={}&upstream=false&downstream=false&future=false&past=false'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url)
        self.check_content_in_response('Wait a minute', resp)

    def test_clear(self):
        url = ('clear?task_id=runme_1&dag_id=example_bash_operator&'
               'execution_date={}&upstream=false&downstream=false&future=false&past=false'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url)
        self.check_content_in_response(['example_bash_operator', 'Wait a minute'], resp)

    def test_run(self):
        url = ('run?task_id=runme_0&dag_id=example_bash_operator&ignore_all_deps=false&'
               'ignore_ti_state=true&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url)
        self.check_content_in_response('', resp, resp_code=302)

    def test_refresh(self):
        resp = self.client.get('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('', resp, resp_code=302)


class TestConfigurationView(TestBase):
    def test_configuration(self):
        resp = self.client.get('configuration', follow_redirects=True)
        self.check_content_in_response(
            ['Airflow Configuration', 'Running Configuration'], resp)


class TestVersionView(TestBase):
    def test_version(self):
        resp = self.client.get('version', follow_redirects=True)
        self.check_content_in_response('Version Info', resp)


if __name__ == '__main__':
    unittest.main()
