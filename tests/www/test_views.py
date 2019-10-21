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

import copy
import io
import json
import logging.config
import os
import re
import shutil
import sys
import tempfile
import unittest
import urllib
from datetime import timedelta
from unittest import mock
from urllib.parse import quote_plus

import jinja2
from flask import Markup, url_for
from parameterized import parameterized
from werkzeug.test import Client
from werkzeug.wrappers import BaseResponse

from airflow import models, settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.configuration import conf
from airflow.executors.celery_executor import CeleryExecutor
from airflow.jobs import BaseJob
from airflow.models import DAG, BaseOperator, Connection, DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperatorLink
from airflow.operators.dummy_operator import DummyOperator
from airflow.settings import Session
from airflow.ti_deps.dep_context import QUEUEABLE_STATES, RUNNABLE_STATES
from airflow.utils import dates, timezone
from airflow.utils.db import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.www import app as application
from tests.test_utils.config import conf_vars


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app, cls.appbuilder = application.create_app(session=Session, testing=True)
        cls.app.config['WTF_CSRF_ENABLED'] = False
        cls.app.jinja_env.undefined = jinja2.StrictUndefined
        settings.configure_orm()
        cls.session = Session

    def setUp(self):
        self.client = self.app.test_client()
        self.login()

    def login(self):
        role_admin = self.appbuilder.sm.find_role('Admin')
        tester = self.appbuilder.sm.find_user(username='test')
        if not tester:
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

    @classmethod
    def clear_table(cls, model):
        with create_session() as session:
            session.query(model).delete()

    def check_content_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        self.assertEqual(resp_code, resp.status_code)
        if isinstance(text, list):
            for kw in text:
                self.assertIn(kw, resp_html)
        else:
            self.assertIn(text, resp_html)

    def check_content_not_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        self.assertEqual(resp_code, resp.status_code)
        if isinstance(text, list):
            for kw in text:
                self.assertNotIn(kw, resp_html)
        else:
            self.assertNotIn(text, resp_html)

    def percent_encode(self, obj):
        return urllib.parse.quote_plus(str(obj))


class TestConnectionModelView(TestBase):
    def setUp(self):
        super().setUp()
        self.connection = {
            'conn_id': 'test_conn',
            'conn_type': 'http',
            'host': 'localhost',
            'port': 8080,
            'username': 'root',
            'password': 'admin'
        }

    def tearDown(self):
        self.clear_table(Connection)
        super().tearDown()

    def test_create_connection(self):
        resp = self.client.post('/connection/add',
                                data=self.connection,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)


class TestVariableModelView(TestBase):
    def setUp(self):
        super().setUp()
        self.variable = {
            'key': 'test_key',
            'val': 'text_val',
            'is_encrypted': True
        }

    def tearDown(self):
        self.clear_table(models.Variable)
        super().tearDown()

    def test_can_handle_error_on_decrypt(self):

        # create valid variable
        resp = self.client.post('/variable/add',
                                data=self.variable,
                                follow_redirects=True)

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

    def test_import_variables_no_file(self):
        resp = self.client.post('/variable/varimport',
                                follow_redirects=True)
        self.check_content_in_response('Missing file or syntax error.', resp)

    def test_import_variables_failed(self):
        content = '{"str_key": "str_value"}'

        with mock.patch('airflow.models.Variable.set') as set_mock:
            set_mock.side_effect = UnicodeEncodeError
            self.assertEqual(self.session.query(models.Variable).count(), 0)

            try:
                # python 3+
                bytes_content = io.BytesIO(bytes(content, encoding='utf-8'))
            except TypeError:
                # python 2.7
                bytes_content = io.BytesIO(bytes(content))

            resp = self.client.post('/variable/varimport',
                                    data={'file': (bytes_content, 'test.json')},
                                    follow_redirects=True)
            self.check_content_in_response('1 variable(s) failed to be updated.', resp)

    def test_import_variables_success(self):
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
        super().setUp()
        self.pool = {
            'pool': 'test-pool',
            'slots': 777,
            'description': 'test-pool-description',
        }

    def tearDown(self):
        self.clear_table(models.Pool)
        super().tearDown()

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

    def test_odd_name(self):
        self.pool['pool'] = 'test-pool<script></script>'
        self.session.add(models.Pool(**self.pool))
        self.session.commit()
        resp = self.client.get('/pool/list/')
        self.check_content_in_response('test-pool&lt;script&gt;', resp)
        self.check_content_not_in_response('test-pool<script>', resp)

    def test_list(self):
        self.pool['pool'] = 'test-pool'
        self.session.add(models.Pool(**self.pool))
        self.session.commit()
        resp = self.client.get('/pool/list/')
        # We should see this link
        with self.app.test_request_context():
            url = url_for('TaskInstanceModelView.list', _flt_3_pool='test-pool', _flt_3_state='running')
            used_tag = Markup("<a href='{url}'>{slots}</a>").format(url=url, slots=0)

            url = url_for('TaskInstanceModelView.list', _flt_3_pool='test-pool', _flt_3_state='queued')
            queued_tag = Markup("<a href='{url}'>{slots}</a>").format(url=url, slots=0)
        self.check_content_in_response(used_tag, resp)
        self.check_content_in_response(queued_tag, resp)


class TestMountPoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        application.app = None
        application.appbuilder = None
        conf.set("webserver", "base_url", "http://localhost/test")
        app = application.cached_app(config={'WTF_CSRF_ENABLED': False}, session=Session, testing=True)
        cls.client = Client(app, BaseResponse)

    @classmethod
    def tearDownClass(cls):
        application.app = None
        application.appbuilder = None

    def test_mount(self):
        # Test an endpoint that doesn't need auth!
        resp = self.client.get('/test/health')
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"healthy", resp.data)

    def test_not_found(self):
        resp = self.client.get('/', follow_redirects=True)
        self.assertEqual(resp.status_code, 404)

    def test_index(self):
        resp = self.client.get('/test/')
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.headers['Location'], 'http://localhost/test/home')


class TestAirflowBaseViews(TestBase):
    EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)
    run_id = "test_{}".format(models.DagRun.id_for_date(EXAMPLE_DAG_DEFAULT_DATE))

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dagbag = models.DagBag(include_examples=True)
        for dag in dagbag.dags.values():
            dag.sync_to_db()

    def setUp(self):
        super().setUp()
        self.logout()
        self.login()
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
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.xcom_dagrun = self.xcom_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

    def test_index(self):
        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('DAGs', resp)

    def test_health(self):

        # case-1: healthy scheduler status
        last_scheduler_heartbeat_for_testing_1 = timezone.utcnow()
        self.session.add(BaseJob(job_type='SchedulerJob',
                                 state='running',
                                 latest_heartbeat=last_scheduler_heartbeat_for_testing_1))
        self.session.commit()

        resp_json = json.loads(self.client.get('health', follow_redirects=True).data.decode('utf-8'))

        self.assertEqual('healthy', resp_json['metadatabase']['status'])
        self.assertEqual('healthy', resp_json['scheduler']['status'])
        self.assertEqual(last_scheduler_heartbeat_for_testing_1.isoformat(),
                         resp_json['scheduler']['latest_scheduler_heartbeat'])

        self.session.query(BaseJob).\
            filter(BaseJob.job_type == 'SchedulerJob',
                   BaseJob.state == 'running',
                   BaseJob.latest_heartbeat == last_scheduler_heartbeat_for_testing_1).\
            delete()
        self.session.commit()

        # case-2: unhealthy scheduler status - scenario 1 (SchedulerJob is running too slowly)
        last_scheduler_heartbeat_for_testing_2 = timezone.utcnow() - timedelta(minutes=1)
        (self.session
             .query(BaseJob)
             .filter(BaseJob.job_type == 'SchedulerJob')
             .update({'latest_heartbeat': last_scheduler_heartbeat_for_testing_2 - timedelta(seconds=1)}))
        self.session.add(BaseJob(job_type='SchedulerJob',
                                 state='running',
                                 latest_heartbeat=last_scheduler_heartbeat_for_testing_2))
        self.session.commit()

        resp_json = json.loads(self.client.get('health', follow_redirects=True).data.decode('utf-8'))

        self.assertEqual('healthy', resp_json['metadatabase']['status'])
        self.assertEqual('unhealthy', resp_json['scheduler']['status'])
        self.assertEqual(last_scheduler_heartbeat_for_testing_2.isoformat(),
                         resp_json['scheduler']['latest_scheduler_heartbeat'])

        self.session.query(BaseJob).\
            filter(BaseJob.job_type == 'SchedulerJob',
                   BaseJob.state == 'running',
                   BaseJob.latest_heartbeat == last_scheduler_heartbeat_for_testing_2).\
            delete()
        self.session.commit()

        # case-3: unhealthy scheduler status - scenario 2 (no running SchedulerJob)
        self.session.query(BaseJob).\
            filter(BaseJob.job_type == 'SchedulerJob',
                   BaseJob.state == 'running').\
            delete()
        self.session.commit()

        resp_json = json.loads(self.client.get('health', follow_redirects=True).data.decode('utf-8'))

        self.assertEqual('healthy', resp_json['metadatabase']['status'])
        self.assertEqual('unhealthy', resp_json['scheduler']['status'])
        self.assertIsNone(None, resp_json['scheduler']['latest_scheduler_heartbeat'])

    def test_home(self):
        resp = self.client.get('home', follow_redirects=True)
        self.check_content_in_response('DAGs', resp)

    def test_task(self):
        url = ('task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_xcom(self):
        url = ('xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_rendered(self):
        url = ('rendered?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

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

    def test_dag_details_subdag(self):
        url = 'dag_details?dag_id=example_subdag_operator.section-1'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG details', resp)

    def test_graph(self):
        url = 'graph?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_last_dagruns(self):
        resp = self.client.get('last_dagruns', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_tree(self):
        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_tree_subdag(self):
        url = 'tree?dag_id=example_subdag_operator.section-1'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('section-1-task-1', resp)

    def test_duration(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_duration_missing(self):
        url = 'duration?days=30&dag_id=missing_dag'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('seems to be missing', resp)

    def test_tries(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_landing_times(self):
        url = 'landing_times?days=30&dag_id=example_bash_operator'
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

    def test_failed(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post("failed", data=form)
        self.check_content_in_response('Wait a minute', resp)

    def test_success(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post('success', data=form)
        self.check_content_in_response('Wait a minute', resp)

    def test_clear(self):
        form = dict(
            task_id="runme_1",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            only_failed="false",
        )
        resp = self.client.post("clear", data=form)
        self.check_content_in_response(['example_bash_operator', 'Wait a minute'], resp)

    def test_run(self):
        form = dict(
            task_id="runme_0",
            dag_id="example_bash_operator",
            ignore_all_deps="false",
            ignore_ti_state="true",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
        )
        resp = self.client.post('run', data=form)
        self.check_content_in_response('', resp, resp_code=302)

    @mock.patch('airflow.executors.get_default_executor')
    def test_run_with_runnable_states(self, get_default_executor_function):
        executor = CeleryExecutor()
        executor.heartbeat = lambda: True
        get_default_executor_function.return_value = executor

        task_id = 'runme_0'

        for state in RUNNABLE_STATES:
            self.session.query(models.TaskInstance) \
                .filter(models.TaskInstance.task_id == task_id) \
                .update({'state': state, 'end_date': timezone.utcnow()})
            self.session.commit()

            form = dict(
                task_id=task_id,
                dag_id="example_bash_operator",
                ignore_all_deps="false",
                ignore_ti_state="false",
                execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
                origin='/home'
            )
            resp = self.client.post('run', data=form, follow_redirects=True)

            self.check_content_in_response('', resp, resp_code=200)

            msg = "Task is in the &#39;{}&#39; state which is not a valid state for execution. " \
                  .format(state) + "The task must be cleared in order to be run"
            self.assertFalse(re.search(msg, resp.get_data(as_text=True)))

    @mock.patch('airflow.executors.get_default_executor')
    def test_run_with_not_runnable_states(self, get_default_executor_function):
        get_default_executor_function.return_value = CeleryExecutor()

        task_id = 'runme_0'

        for state in QUEUEABLE_STATES:
            self.assertFalse(state in RUNNABLE_STATES)

            self.session.query(models.TaskInstance) \
                .filter(models.TaskInstance.task_id == task_id) \
                .update({'state': state, 'end_date': timezone.utcnow()})
            self.session.commit()

            form = dict(
                task_id=task_id,
                dag_id="example_bash_operator",
                ignore_all_deps="false",
                ignore_ti_state="false",
                execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
                origin='/home'
            )
            resp = self.client.post('run', data=form, follow_redirects=True)

            self.check_content_in_response('', resp, resp_code=200)

            msg = "Task is in the &#39;{}&#39; state which is not a valid state for execution. " \
                  .format(state) + "The task must be cleared in order to be run"
            self.assertTrue(re.search(msg, resp.get_data(as_text=True)))

    def test_refresh(self):
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('', resp, resp_code=302)

    def test_refresh_all(self):
        resp = self.client.post("/refresh_all",
                                follow_redirects=True)
        self.check_content_in_response('', resp, resp_code=200)

    def test_delete_dag_button_normal(self):
        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('/delete?dag_id=example_bash_operator', resp)
        self.check_content_in_response("return confirmDeleteDag(this, 'example_bash_operator')", resp)

    def test_delete_dag_button_for_dag_on_scheduler_only(self):
        # Test for JIRA AIRFLOW-3233 (PR 4069):
        # The delete-dag URL should be generated correctly for DAGs
        # that exist on the scheduler (DB) but not the webserver DagBag

        test_dag_id = "non_existent_dag"

        DM = models.DagModel
        self.session.query(DM).filter(DM.dag_id == 'example_bash_operator').update({'dag_id': test_dag_id})
        self.session.commit()

        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('/delete?dag_id={}'.format(test_dag_id), resp)
        self.check_content_in_response("return confirmDeleteDag(this, '{}')".format(test_dag_id), resp)

        self.session.query(DM).filter(DM.dag_id == test_dag_id).update({'dag_id': 'example_bash_operator'})
        self.session.commit()


class TestConfigurationView(TestBase):
    def test_configuration_do_not_expose_config(self):
        self.logout()
        self.login()
        with conf_vars({('webserver', 'expose_config'): 'False'}):
            resp = self.client.get('configuration', follow_redirects=True)
        self.check_content_in_response(
            ['Airflow Configuration', '# Your Airflow administrator chose not to expose the configuration, '
                                      'most likely for security reasons.'], resp)

    def test_configuration_expose_config(self):
        self.logout()
        self.login()
        with conf_vars({('webserver', 'expose_config'): 'True'}):
            resp = self.client.get('configuration', follow_redirects=True)
        self.check_content_in_response(
            ['Airflow Configuration', 'Running Configuration'], resp)


class TestLogView(TestBase):
    DAG_ID = 'dag_for_testing_log_view'
    TASK_ID = 'task_for_testing_log_view'
    DEFAULT_DATE = timezone.datetime(2017, 9, 1)
    ENDPOINT = 'log?dag_id={dag_id}&task_id={task_id}&' \
               'execution_date={execution_date}'.format(dag_id=DAG_ID,
                                                        task_id=TASK_ID,
                                                        execution_date=DEFAULT_DATE)

    def setUp(self):
        # Make sure that the configure_logging is not cached
        self.old_modules = dict(sys.modules)

        # Create a custom logging configuration
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        logging_config['handlers']['task']['base_log_folder'] = os.path.normpath(
            os.path.join(current_dir, 'test_logs'))

        logging_config['handlers']['task']['filename_template'] = \
            '{{ ti.dag_id }}/{{ ti.task_id }}/' \
            '{{ ts | replace(":", ".") }}/{{ try_number }}.log'

        # Write the custom logging configuration to a file
        self.settings_folder = tempfile.mkdtemp()
        settings_file = os.path.join(self.settings_folder, "airflow_local_settings.py")
        new_logging_file = "LOGGING_CONFIG = {}".format(logging_config)
        with open(settings_file, 'w') as handle:
            handle.writelines(new_logging_file)
        sys.path.append(self.settings_folder)
        conf.set('core', 'logging_config_class', 'airflow_local_settings.LOGGING_CONFIG')

        self.app, self.appbuilder = application.create_app(session=Session, testing=True)
        self.app.config['WTF_CSRF_ENABLED'] = False
        self.client = self.app.test_client()
        settings.configure_orm()
        self.login()

        from airflow.www.views import dagbag
        dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
        dag.sync_to_db()
        task = DummyOperator(task_id=self.TASK_ID, dag=dag)
        dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
        with create_session() as session:
            self.ti = TaskInstance(task=task, execution_date=self.DEFAULT_DATE)
            self.ti.try_number = 1
            session.merge(self.ti)

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        self.clear_table(TaskInstance)

        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for m in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[m]

        sys.path.remove(self.settings_folder)
        shutil.rmtree(self.settings_folder)
        conf.set('core', 'logging_config_class', '')

        self.logout()
        super().tearDown()

    @parameterized.expand([
        [State.NONE, 0, 0],
        [State.UP_FOR_RETRY, 2, 2],
        [State.UP_FOR_RESCHEDULE, 0, 1],
        [State.UP_FOR_RESCHEDULE, 1, 2],
        [State.RUNNING, 1, 1],
        [State.SUCCESS, 1, 1],
        [State.FAILED, 3, 3],
    ])
    def test_get_file_task_log(self, state, try_number, expected_num_logs_visible):
        with create_session() as session:
            self.ti.state = state
            self.ti.try_number = try_number
            session.merge(self.ti)

        response = self.client.get(
            TestLogView.ENDPOINT, data=dict(
                username='test',
                password='test'), follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('Log by attempts', response.data.decode('utf-8'))
        for num in range(1, expected_num_logs_visible + 1):
            self.assertIn('try-{}'.format(num), response.data.decode('utf-8'))
        self.assertNotIn('try-0', response.data.decode('utf-8'))
        self.assertNotIn('try-{}'.format(expected_num_logs_visible + 1), response.data.decode('utf-8'))

    def test_get_logs_with_metadata_as_download_file(self):
        url_template = "get_logs_with_metadata?dag_id={}&" \
                       "task_id={}&execution_date={}&" \
                       "try_number={}&metadata={}&format=file"
        try_number = 1
        url = url_template.format(self.DAG_ID,
                                  self.TASK_ID,
                                  quote_plus(self.DEFAULT_DATE.isoformat()),
                                  try_number,
                                  json.dumps({}))
        response = self.client.get(url)
        expected_filename = '{}/{}/{}/{}.log'.format(self.DAG_ID,
                                                     self.TASK_ID,
                                                     self.DEFAULT_DATE.isoformat(),
                                                     try_number)

        content_disposition = response.headers.get('Content-Disposition')
        self.assertTrue(content_disposition.startswith('attachment'))
        self.assertTrue(expected_filename in content_disposition)
        self.assertEqual(200, response.status_code)
        self.assertIn('Log for testing.', response.data.decode('utf-8'))

    def test_get_logs_with_metadata_as_download_large_file(self):
        with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read") as read_mock:
            first_return = (['1st line'], [{}])
            second_return = (['2nd line'], [{'end_of_log': False}])
            third_return = (['3rd line'], [{'end_of_log': True}])
            fourth_return = (['should never be read'], [{'end_of_log': True}])
            read_mock.side_effect = [first_return, second_return, third_return, fourth_return]
            url_template = "get_logs_with_metadata?dag_id={}&" \
                           "task_id={}&execution_date={}&" \
                           "try_number={}&metadata={}&format=file"
            try_number = 1
            url = url_template.format(self.DAG_ID,
                                      self.TASK_ID,
                                      quote_plus(self.DEFAULT_DATE.isoformat()),
                                      try_number,
                                      json.dumps({}))
            response = self.client.get(url)

            self.assertIn('1st line', response.data.decode('utf-8'))
            self.assertIn('2nd line', response.data.decode('utf-8'))
            self.assertIn('3rd line', response.data.decode('utf-8'))
            self.assertNotIn('should never be read', response.data.decode('utf-8'))

    def test_get_logs_with_metadata(self):
        url_template = "get_logs_with_metadata?dag_id={}&" \
                       "task_id={}&execution_date={}&" \
                       "try_number={}&metadata={}"
        response = \
            self.client.get(url_template.format(self.DAG_ID,
                                                self.TASK_ID,
                                                quote_plus(self.DEFAULT_DATE.isoformat()),
                                                1,
                                                json.dumps({})), data=dict(
                                                    username='test',
                                                    password='test'),
                            follow_redirects=True)

        self.assertIn('"message":', response.data.decode('utf-8'))
        self.assertIn('"metadata":', response.data.decode('utf-8'))
        self.assertIn('Log for testing.', response.data.decode('utf-8'))
        self.assertEqual(200, response.status_code)

    def test_get_logs_with_null_metadata(self):
        url_template = "get_logs_with_metadata?dag_id={}&" \
                       "task_id={}&execution_date={}&" \
                       "try_number={}&metadata=null"
        response = \
            self.client.get(url_template.format(self.DAG_ID,
                                                self.TASK_ID,
                                                quote_plus(self.DEFAULT_DATE.isoformat()),
                                                1), data=dict(
                                                    username='test',
                                                    password='test'),
                            follow_redirects=True)

        self.assertIn('"message":', response.data.decode('utf-8'))
        self.assertIn('"metadata":', response.data.decode('utf-8'))
        self.assertIn('Log for testing.', response.data.decode('utf-8'))
        self.assertEqual(200, response.status_code)


class TestVersionView(TestBase):
    def test_version(self):
        resp = self.client.get('version', data=dict(
            username='test',
            password='test'
        ), follow_redirects=True)
        self.check_content_in_response('Version Info', resp)


class ViewWithDateTimeAndNumRunsAndDagRunsFormTester:
    DAG_ID = 'dag_for_testing_dt_nr_dr_form'
    DEFAULT_DATE = datetime(2017, 9, 1)
    RUNS_DATA = [
        ('dag_run_for_testing_dt_nr_dr_form_4', datetime(2018, 4, 4)),
        ('dag_run_for_testing_dt_nr_dr_form_3', datetime(2018, 3, 3)),
        ('dag_run_for_testing_dt_nr_dr_form_2', datetime(2018, 2, 2)),
        ('dag_run_for_testing_dt_nr_dr_form_1', datetime(2018, 1, 1)),
    ]

    def __init__(self, test, endpoint):
        self.test = test
        self.endpoint = endpoint

    def setUp(self):
        from airflow.www.views import dagbag
        from airflow.utils.state import State
        dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
        dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
        self.runs = []
        for rd in self.RUNS_DATA:
            run = dag.create_dagrun(
                run_id=rd[0],
                execution_date=rd[1],
                state=State.SUCCESS,
                external_trigger=True
            )
            self.runs.append(run)

    def tearDown(self):
        self.test.session.query(DagRun).filter(
            DagRun.dag_id == self.DAG_ID).delete()
        self.test.session.commit()
        self.test.session.close()

    def assertBaseDateAndNumRuns(self, base_date, num_runs, data):
        self.test.assertNotIn('name="base_date" value="{}"'.format(base_date), data)
        self.test.assertNotIn('<option selected="" value="{}">{}</option>'.format(
            num_runs, num_runs), data)

    def assertRunIsNotInDropdown(self, run, data):
        self.test.assertNotIn(run.execution_date.isoformat(), data)
        self.test.assertNotIn(run.run_id, data)

    def assertRunIsInDropdownNotSelected(self, run, data):
        self.test.assertIn('<option value="{}">{}</option>'.format(
            run.execution_date.isoformat(), run.run_id), data)

    def assertRunIsSelected(self, run, data):
        self.test.assertIn('<option selected value="{}">{}</option>'.format(
            run.execution_date.isoformat(), run.run_id), data)

    def test_with_default_parameters(self):
        """
        Tests view with no URL parameter.
        Should show all dag runs in the drop down.
        Should select the latest dag run.
        Should set base date to current date (not asserted)
        """
        response = self.test.client.get(
            self.endpoint, data=dict(
                username='test',
                password='test'), follow_redirects=True)
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.test.assertIn('Base date:', data)
        self.test.assertIn('Number of runs:', data)
        self.assertRunIsSelected(self.runs[0], data)
        self.assertRunIsInDropdownNotSelected(self.runs[1], data)
        self.assertRunIsInDropdownNotSelected(self.runs[2], data)
        self.assertRunIsInDropdownNotSelected(self.runs[3], data)

    def test_with_execution_date_parameter_only(self):
        """
        Tests view with execution_date URL parameter.
        Scenario: click link from dag runs view.
        Should only show dag runs older than execution_date in the drop down.
        Should select the particular dag run.
        Should set base date to execution date.
        """
        response = self.test.client.get(
            self.endpoint + '&execution_date={}'.format(
                self.runs[1].execution_date.isoformat()),
            data=dict(
                username='test',
                password='test'
            ), follow_redirects=True
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assertBaseDateAndNumRuns(
            self.runs[1].execution_date,
            conf.getint('webserver', 'default_dag_run_display_number'),
            data)
        self.assertRunIsNotInDropdown(self.runs[0], data)
        self.assertRunIsSelected(self.runs[1], data)
        self.assertRunIsInDropdownNotSelected(self.runs[2], data)
        self.assertRunIsInDropdownNotSelected(self.runs[3], data)

    def test_with_base_date_and_num_runs_parmeters_only(self):
        """
        Tests view with base_date and num_runs URL parameters.
        Should only show dag runs older than base_date in the drop down,
        limited to num_runs.
        Should select the latest dag run.
        Should set base date and num runs to submitted values.
        """
        response = self.test.client.get(
            self.endpoint + '&base_date={}&num_runs=2'.format(
                self.runs[1].execution_date.isoformat()),
            data=dict(
                username='test',
                password='test'
            ), follow_redirects=True
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assertBaseDateAndNumRuns(self.runs[1].execution_date, 2, data)
        self.assertRunIsNotInDropdown(self.runs[0], data)
        self.assertRunIsSelected(self.runs[1], data)
        self.assertRunIsInDropdownNotSelected(self.runs[2], data)
        self.assertRunIsNotInDropdown(self.runs[3], data)

    def test_with_base_date_and_num_runs_and_execution_date_outside(self):
        """
        Tests view with base_date and num_runs and execution-date URL parameters.
        Scenario: change the base date and num runs and press "Go",
        the selected execution date is outside the new range.
        Should only show dag runs older than base_date in the drop down.
        Should select the latest dag run within the range.
        Should set base date and num runs to submitted values.
        """
        response = self.test.client.get(
            self.endpoint + '&base_date={}&num_runs=42&execution_date={}'.format(
                self.runs[1].execution_date.isoformat(),
                self.runs[0].execution_date.isoformat()),
            data=dict(
                username='test',
                password='test'
            ), follow_redirects=True
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assertBaseDateAndNumRuns(self.runs[1].execution_date, 42, data)
        self.assertRunIsNotInDropdown(self.runs[0], data)
        self.assertRunIsSelected(self.runs[1], data)
        self.assertRunIsInDropdownNotSelected(self.runs[2], data)
        self.assertRunIsInDropdownNotSelected(self.runs[3], data)

    def test_with_base_date_and_num_runs_and_execution_date_within(self):
        """
        Tests view with base_date and num_runs and execution-date URL parameters.
        Scenario: change the base date and num runs and press "Go",
        the selected execution date is within the new range.
        Should only show dag runs older than base_date in the drop down.
        Should select the dag run with the execution date.
        Should set base date and num runs to submitted values.
        """
        response = self.test.client.get(
            self.endpoint + '&base_date={}&num_runs=5&execution_date={}'.format(
                self.runs[2].execution_date.isoformat(),
                self.runs[3].execution_date.isoformat()),
            data=dict(
                username='test',
                password='test'
            ), follow_redirects=True
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assertBaseDateAndNumRuns(self.runs[2].execution_date, 5, data)
        self.assertRunIsNotInDropdown(self.runs[0], data)
        self.assertRunIsNotInDropdown(self.runs[1], data)
        self.assertRunIsInDropdownNotSelected(self.runs[2], data)
        self.assertRunIsSelected(self.runs[3], data)


class TestGraphView(TestBase):
    GRAPH_ENDPOINT = '/graph?dag_id={dag_id}'.format(
        dag_id=ViewWithDateTimeAndNumRunsAndDagRunsFormTester.DAG_ID
    )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.tester = ViewWithDateTimeAndNumRunsAndDagRunsFormTester(
            self, self.GRAPH_ENDPOINT)
        self.tester.setUp()

    def tearDown(self):
        self.tester.tearDown()
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_dt_nr_dr_form_default_parameters(self):
        self.tester.test_with_default_parameters()

    def test_dt_nr_dr_form_with_execution_date_parameter_only(self):
        self.tester.test_with_execution_date_parameter_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_parmeters_only(self):
        self.tester.test_with_base_date_and_num_runs_parmeters_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_outside(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_outside()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_within(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_within()


class TestGanttView(TestBase):
    GANTT_ENDPOINT = '/gantt?dag_id={dag_id}'.format(
        dag_id=ViewWithDateTimeAndNumRunsAndDagRunsFormTester.DAG_ID
    )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.tester = ViewWithDateTimeAndNumRunsAndDagRunsFormTester(
            self, self.GANTT_ENDPOINT)
        self.tester.setUp()

    def tearDown(self):
        self.tester.tearDown()
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_dt_nr_dr_form_default_parameters(self):
        self.tester.test_with_default_parameters()

    def test_dt_nr_dr_form_with_execution_date_parameter_only(self):
        self.tester.test_with_execution_date_parameter_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_parmeters_only(self):
        self.tester.test_with_base_date_and_num_runs_parmeters_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_outside(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_outside()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_within(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_within()


class TestDagACLView(TestBase):
    """
    Test Airflow DAG acl
    """
    default_date = timezone.datetime(2018, 6, 1)
    run_id = "test_{}".format(models.DagRun.id_for_date(default_date))

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dagbag = models.DagBag(include_examples=True)
        for dag in dagbag.dags.values():
            dag.sync_to_db()

    def cleanup_dagruns(self):
        DR = models.DagRun
        dag_ids = ['example_bash_operator',
                   'example_subdag_operator']
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

    def setUp(self):
        super().setUp()
        self.cleanup_dagruns()
        self.prepare_dagruns()
        self.logout()
        self.appbuilder.sm.sync_roles()
        self.add_permission_for_role()

    def login(self, username=None, password=None):
        role_admin = self.appbuilder.sm.find_role('Admin')
        tester = self.appbuilder.sm.find_user(username='test')
        if not tester:
            self.appbuilder.sm.add_user(
                username='test',
                first_name='test',
                last_name='test',
                email='test@fab.org',
                role=role_admin,
                password='test')

        role_user = self.appbuilder.sm.find_role('User')
        test_user = self.appbuilder.sm.find_user(username='test_user')
        if not test_user:
            self.appbuilder.sm.add_user(
                username='test_user',
                first_name='test_user',
                last_name='test_user',
                email='test_user@fab.org',
                role=role_user,
                password='test_user')

        role_viewer = self.appbuilder.sm.find_role('Viewer')
        test_viewer = self.appbuilder.sm.find_user(username='test_viewer')
        if not test_viewer:
            self.appbuilder.sm.add_user(
                username='test_viewer',
                first_name='test_viewer',
                last_name='test_viewer',
                email='test_viewer@fab.org',
                role=role_viewer,
                password='test_viewer')

        dag_acl_role = self.appbuilder.sm.add_role('dag_acl_tester')
        dag_tester = self.appbuilder.sm.find_user(username='dag_tester')
        if not dag_tester:
            self.appbuilder.sm.add_user(
                username='dag_tester',
                first_name='dag_test',
                last_name='dag_test',
                email='dag_test@fab.org',
                role=dag_acl_role,
                password='dag_test')

        # create an user without permission
        dag_no_role = self.appbuilder.sm.add_role('dag_acl_faker')
        dag_faker = self.appbuilder.sm.find_user(username='dag_faker')
        if not dag_faker:
            self.appbuilder.sm.add_user(
                username='dag_faker',
                first_name='dag_faker',
                last_name='dag_faker',
                email='dag_fake@fab.org',
                role=dag_no_role,
                password='dag_faker')

        # create an user with only read permission
        dag_read_only_role = self.appbuilder.sm.add_role('dag_acl_read_only')
        dag_read_only = self.appbuilder.sm.find_user(username='dag_read_only')
        if not dag_read_only:
            self.appbuilder.sm.add_user(
                username='dag_read_only',
                first_name='dag_read_only',
                last_name='dag_read_only',
                email='dag_read_only@fab.org',
                role=dag_read_only_role,
                password='dag_read_only')

        # create an user that has all dag access
        all_dag_role = self.appbuilder.sm.add_role('all_dag_role')
        all_dag_tester = self.appbuilder.sm.find_user(username='all_dag_user')
        if not all_dag_tester:
            self.appbuilder.sm.add_user(
                username='all_dag_user',
                first_name='all_dag_user',
                last_name='all_dag_user',
                email='all_dag_user@fab.org',
                role=all_dag_role,
                password='all_dag_user')

        user = username if username else 'dag_tester'
        passwd = password if password else 'dag_test'

        return self.client.post('/login/', data=dict(
            username=user,
            password=passwd
        ))

    def logout(self):
        return self.client.get('/logout/')

    def add_permission_for_role(self):
        self.logout()
        self.login(username='test',
                   password='test')
        perm_on_dag = self.appbuilder.sm.\
            find_permission_view_menu('can_dag_edit', 'example_bash_operator')
        dag_tester_role = self.appbuilder.sm.find_role('dag_acl_tester')
        self.appbuilder.sm.add_permission_role(dag_tester_role, perm_on_dag)

        perm_on_all_dag = self.appbuilder.sm.\
            find_permission_view_menu('can_dag_edit', 'all_dags')
        all_dag_role = self.appbuilder.sm.find_role('all_dag_role')
        self.appbuilder.sm.add_permission_role(all_dag_role, perm_on_all_dag)

        role_user = self.appbuilder.sm.find_role('User')
        self.appbuilder.sm.add_permission_role(role_user, perm_on_all_dag)

        read_only_perm_on_dag = self.appbuilder.sm.\
            find_permission_view_menu('can_dag_read', 'example_bash_operator')
        dag_read_only_role = self.appbuilder.sm.find_role('dag_acl_read_only')
        self.appbuilder.sm.add_permission_role(dag_read_only_role, read_only_perm_on_dag)

    def test_permission_exist(self):
        self.logout()
        self.login(username='test',
                   password='test')
        test_view_menu = self.appbuilder.sm.find_view_menu('example_bash_operator')
        perms_views = self.appbuilder.sm.find_permissions_view_menu(test_view_menu)
        self.assertEqual(len(perms_views), 2)
        # each dag view will create one write, and one read permission
        self.assertTrue(str(perms_views[0]).startswith('can dag'))
        self.assertTrue(str(perms_views[1]).startswith('can dag'))

    def test_role_permission_associate(self):
        self.logout()
        self.login(username='test',
                   password='test')
        test_role = self.appbuilder.sm.find_role('dag_acl_tester')
        perms = {str(perm) for perm in test_role.permissions}
        self.assertIn('can dag edit on example_bash_operator', perms)
        self.assertNotIn('can dag read on example_bash_operator', perms)

    def test_index_success(self):
        self.logout()
        self.login()
        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_index_failure(self):
        self.logout()
        self.login()
        resp = self.client.get('/', follow_redirects=True)
        # The user can only access/view example_bash_operator dag.
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_index_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.get('/', follow_redirects=True)
        # The all dag user can access/view all dags.
        self.check_content_in_response('example_subdag_operator', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_dag_stats_success(self):
        self.logout()
        self.login()
        resp = self.client.get('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_dag_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.get('dag_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_dag_stats_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.get('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_task_stats_success(self):
        self.logout()
        self.login()
        resp = self.client.get('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_task_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.get('task_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_task_stats_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.get('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_code_success(self):
        self.logout()
        self.login()
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_code_failure(self):
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_code_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

        url = 'code?dag_id=example_subdag_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_dag_details_success(self):
        self.logout()
        self.login()
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG details', resp)

    def test_dag_details_failure(self):
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('DAG details', resp)

    def test_dag_details_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

        url = 'dag_details?dag_id=example_subdag_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_rendered_success(self):
        self.logout()
        self.login()
        url = ('rendered?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_rendered_failure(self):
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        url = ('rendered?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Rendered Template', resp)

    def test_rendered_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        url = ('rendered?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_task_success(self):
        self.logout()
        self.login()
        url = ('task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_task_failure(self):
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        url = ('task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Task Instance Details', resp)

    def test_task_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        url = ('task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_xcom_success(self):
        self.logout()
        self.login()
        url = ('xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_xcom_failure(self):
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        url = ('xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('XCom', resp)

    def test_xcom_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        url = ('xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_run_success(self):
        self.logout()
        self.login()
        form = dict(
            task_id="runme_0",
            dag_id="example_bash_operator",
            ignore_all_deps="false",
            ignore_ti_state="true",
            execution_date=self.default_date,
        )
        resp = self.client.post('run', data=form)
        self.check_content_in_response('', resp, resp_code=302)

    def test_run_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        form = dict(
            task_id="runme_0",
            dag_id="example_bash_operator",
            ignore_all_deps="false",
            ignore_ti_state="true",
            execution_date=self.default_date
        )
        resp = self.client.post('run', data=form)
        self.check_content_in_response('', resp, resp_code=302)

    def test_blocked_success(self):
        url = 'blocked'
        self.logout()
        self.login()
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_blocked_success_for_all_dag_user(self):
        url = 'blocked'
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_failed_success(self):
        self.logout()
        self.login()
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.default_date,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post('failed', data=form)
        self.check_content_in_response('Redirecting', resp, 302)

    def test_duration_success(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        self.logout()
        self.login()
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_duration_failure(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        self.logout()
        # login as an user without permissions
        self.login(username='dag_faker',
                   password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_tries_success(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        self.logout()
        self.login()
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_tries_failure(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        self.logout()
        # login as an user without permissions
        self.login(username='dag_faker',
                   password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_landing_times_success(self):
        url = 'landing_times?days=30&dag_id=example_bash_operator'
        self.logout()
        self.login()
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_landing_times_failure(self):
        url = 'landing_times?days=30&dag_id=example_bash_operator'
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_paused_success(self):
        # post request failure won't test
        url = 'paused?dag_id=example_bash_operator&is_paused=false'
        self.logout()
        self.login()
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('OK', resp)

    def test_refresh_success(self):
        self.logout()
        self.login()
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('', resp, resp_code=302)

    def test_gantt_success(self):
        url = 'gantt?dag_id=example_bash_operator'
        self.logout()
        self.login()
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_gantt_failure(self):
        url = 'gantt?dag_id=example_bash_operator'
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_success_fail_for_read_only_role(self):
        # succcess endpoint need can_dag_edit, which read only role can not access
        self.logout()
        self.login(username='dag_read_only',
                   password='dag_read_only')

        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.default_date,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
        )
        resp = self.client.post('success', data=form)
        self.check_content_not_in_response('Wait a minute', resp, resp_code=302)

    def test_tree_success_for_read_only_role(self):
        # tree view only allows can_dag_read, which read only role could access
        self.logout()
        self.login(username='dag_read_only',
                   password='dag_read_only')

        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_log_success(self):
        self.logout()
        self.login()
        url = ('log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Log by attempts', resp)
        url = ('get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
               'execution_date={}&try_number=1&metadata=null'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('"message":', resp)
        self.check_content_in_response('"metadata":', resp)

    def test_log_failure(self):
        self.logout()
        self.login(username='dag_faker',
                   password='dag_faker')
        url = ('log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Log by attempts', resp)
        url = ('get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
               'execution_date={}&try_number=1&metadata=null'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('"message":', resp)
        self.check_content_not_in_response('"metadata":', resp)

    def test_log_success_for_user(self):
        self.logout()
        self.login(username='test_user',
                   password='test_user')
        url = ('log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Log by attempts', resp)
        url = ('get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
               'execution_date={}&try_number=1&metadata=null'
               .format(self.percent_encode(self.default_date)))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('"message":', resp)
        self.check_content_in_response('"metadata":', resp)

    def test_tree_view_for_viewer(self):
        self.logout()
        self.login(username='test_viewer',
                   password='test_viewer')
        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_refresh_failure_for_viewer(self):
        # viewer role can't refresh
        self.logout()
        self.login(username='test_viewer',
                   password='test_viewer')
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('Redirecting', resp, resp_code=302)


class TestTaskInstanceView(TestBase):
    TI_ENDPOINT = '/taskinstance/list/?_flt_0_execution_date={}'

    def test_start_date_filter(self):
        resp = self.client.get(self.TI_ENDPOINT.format(
            self.percent_encode('2018-10-09 22:44:31')))
        # We aren't checking the logic of the date filter itself (that is built
        # in to FAB) but simply that our UTC conversion was run - i.e. it
        # doesn't blow up!
        self.check_content_in_response('List Task Instance', resp)


class TestTriggerDag(TestBase):

    def setUp(self):
        super().setUp()
        self.session = Session()
        models.DagBag().get_dag("example_bash_operator").sync_to_db(session=self.session)

    def test_trigger_dag_button_normal_exist(self):
        resp = self.client.get('/', follow_redirects=True)
        self.assertIn('/trigger?dag_id=example_bash_operator', resp.data.decode('utf-8'))
        self.assertIn("return confirmDeleteDag(this, 'example_bash_operator')", resp.data.decode('utf-8'))

    @unittest.skipIf('mysql' in conf.get('core', 'sql_alchemy_conn'),
                     "flaky when run on mysql")
    def test_trigger_dag_button(self):

        test_dag_id = "example_bash_operator"

        DR = models.DagRun
        self.session.query(DR).delete()
        self.session.commit()

        self.client.post('trigger?dag_id={}'.format(test_dag_id))

        run = self.session.query(DR).filter(DR.dag_id == test_dag_id).first()
        self.assertIsNotNone(run)
        self.assertIn("manual__", run.run_id)


class TestExtraLinks(TestBase):
    def setUp(self):
        from airflow.utils.tests import (
            Dummy2TestOperator, Dummy3TestOperator)
        super().setUp()
        self.ENDPOINT = "extra_links"
        self.DEFAULT_DATE = datetime(2017, 1, 1)

        class RaiseErrorLink(BaseOperatorLink):
            name = 'raise_error'

            def get_link(self, operator, dttm):
                raise ValueError('This is an error')

        class NoResponseLink(BaseOperatorLink):
            name = 'no_response'

            def get_link(self, operator, dttm):
                return None

        class FooBarLink(BaseOperatorLink):
            name = 'foo-bar'

            def get_link(self, operator, dttm):
                return 'http://www.example.com/{0}/{1}/{2}'.format(
                    operator.task_id, 'foo-bar', dttm)

        class AirflowLink(BaseOperatorLink):
            name = 'airflow'

            def get_link(self, operator, dttm):
                return 'https://airflow.apache.org'

        class DummyTestOperator(BaseOperator):

            operator_extra_links = (
                RaiseErrorLink(),
                NoResponseLink(),
                FooBarLink(),
                AirflowLink(),
            )

        self.dag = DAG('dag', start_date=self.DEFAULT_DATE)
        self.task = DummyTestOperator(task_id="some_dummy_task", dag=self.dag)
        self.task_2 = Dummy2TestOperator(task_id="some_dummy_task_2", dag=self.dag)
        self.task_3 = Dummy3TestOperator(task_id="some_dummy_task_3", dag=self.dag)

    def tearDown(self):
        super().tearDown()

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_extra_links_works(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.client.get(
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
    def test_global_extra_links_works(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=github"
            .format(self.ENDPOINT, self.dag.dag_id, self.task.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': 'https://github.com/apache/airflow',
            'error': None
        })

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_operator_extra_link_override_global_extra_link(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=airflow".format(
                self.ENDPOINT, self.dag.dag_id, self.task.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': 'https://airflow.apache.org',
            'error': None
        })

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_extra_links_error_raised(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.client.get(
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

        response = self.client.get(
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

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_operator_extra_link_override_plugin(self, get_dag_function):
        """
        This tests checks if Operator Link (AirflowLink) defined in the Dummy2TestOperator
        is overriden by Airflow Plugin (AirflowLink2).

        AirflowLink returns 'https://airflow.apache.org/' link
        AirflowLink2 returns 'https://airflow.apache.org/1.10.5/' link
        """
        get_dag_function.return_value = self.dag

        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=airflow".format(
                self.ENDPOINT, self.dag.dag_id, self.task_2.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': 'https://airflow.apache.org/1.10.5/',
            'error': None
        })

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_operator_extra_link_multiple_operators(self, get_dag_function):
        """
        This tests checks if Operator Link (AirflowLink2) defined in
        Airflow Plugin (AirflowLink2) is attached to all the list of
        operators defined in the AirflowLink2().operators property

        AirflowLink2 returns 'https://airflow.apache.org/1.10.5/' link
        GoogleLink returns 'https://www.google.com'
        """
        get_dag_function.return_value = self.dag

        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=airflow".format(
                self.ENDPOINT, self.dag.dag_id, self.task_2.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': 'https://airflow.apache.org/1.10.5/',
            'error': None
        })

        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=airflow".format(
                self.ENDPOINT, self.dag.dag_id, self.task_3.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': 'https://airflow.apache.org/1.10.5/',
            'error': None
        })

        # Also check that the other Operator Link defined for this operator exists
        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=google".format(
                self.ENDPOINT, self.dag.dag_id, self.task_3.task_id, self.DEFAULT_DATE),
            follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        response_str = response.data
        if isinstance(response.data, bytes):
            response_str = response_str.decode()
        self.assertEqual(json.loads(response_str), {
            'url': 'https://www.google.com',
            'error': None
        })


class TestDagRunModelView(TestBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        models.DagBag().get_dag("example_bash_operator").sync_to_db(session=cls.session)
        cls.clear_table(models.DagRun)

    def tearDown(self):
        self.clear_table(models.DagRun)

    def test_create_dagrun(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:04:03",
            "run_id": "manual_abc",
        }
        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)

        dr = self.session.query(models.DagRun).one()

        self.assertEqual(dr.execution_date, timezone.convert_to_utc(datetime(2018, 7, 6, 5, 4, 3)))


class TestDecorators(TestBase):
    EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)
    run_id = "test_{}".format(models.DagRun.id_for_date(EXAMPLE_DAG_DEFAULT_DATE))

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dagbag = models.DagBag(include_examples=True)
        for dag in dagbag.dags.values():
            dag.sync_to_db()

    def setUp(self):
        super().setUp()
        self.logout()
        self.login()
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
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.xcom_dagrun = self.xcom_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

    def check_last_log(self, dag_id, event, execution_date=None):
        from airflow.models import Log
        qry = self.session.query(Log.dag_id, Log.task_id, Log.event, Log.execution_date,
                                 Log.owner, Log.extra)
        qry = qry.filter(Log.dag_id == dag_id, Log.event == event)
        if execution_date:
            qry = qry.filter(Log.execution_date == execution_date)
        logs = qry.order_by(Log.dttm.desc()).limit(5).all()
        self.assertGreaterEqual(len(logs), 1)
        self.assertTrue(logs[0].extra)

    def test_action_logging_get(self):
        url = 'graph?dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE))
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

        # In mysql backend, this commit() is needed to write down the logs
        self.session.commit()
        self.check_last_log("example_bash_operator", event="graph",
                            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE)

    def test_action_logging_post(self):
        form = dict(
            task_id="runme_1",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            only_failed="false",
        )
        resp = self.client.post("clear", data=form)
        self.check_content_in_response(['example_bash_operator', 'Wait a minute'], resp)
        # In mysql backend, this commit() is needed to write down the logs
        self.session.commit()
        self.check_last_log("example_bash_operator", event="clear",
                            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE)


if __name__ == '__main__':
    unittest.main()
