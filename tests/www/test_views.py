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
import html
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
from contextlib import contextmanager
from datetime import datetime as dt, timedelta, timezone as tz
from typing import Any, Dict, Generator, List, NamedTuple
from unittest import mock
from urllib.parse import quote_plus

import jinja2
import pytest
from flask import Markup, session as flask_session, template_rendered, url_for
from parameterized import parameterized
from werkzeug.test import Client
from werkzeug.wrappers import BaseResponse

from airflow import models, settings, version
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.configuration import conf
from airflow.executors.celery_executor import CeleryExecutor
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, Connection, DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.settings import Session
from airflow.ti_deps.dependencies_states import QUEUEABLE_STATES, RUNNABLE_STATES
from airflow.utils import dates, timezone
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import using_mysql
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from airflow.www import app as application
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs


class TemplateWithContext(NamedTuple):
    template: jinja2.environment.Template
    context: Dict[str, Any]

    @property
    def name(self):
        return self.template.name

    @property
    def local_context(self):
        """Returns context without global arguments"""
        result = copy.copy(self.context)
        keys_to_delete = [
            # flask.templating._default_template_ctx_processor
            'g',
            'request',
            'session',
            # flask_wtf.csrf.CSRFProtect.init_app
            'csrf_token',
            # flask_login.utils._user_context_processor
            'current_user',
            # flask_appbuilder.baseviews.BaseView.render_template
            'appbuilder',
            'base_template',
            # airflow.www.app.py.create_app (inner method - jinja_globals)
            'server_timezone',
            'default_ui_timezone',
            'hostname',
            'navbar_color',
            'log_fetch_delay_sec',
            'log_auto_tailing_offset',
            'log_animation_speed',
            # airflow.www.static_config.configure_manifest_files
            'url_for_asset',
            # airflow.www.views.AirflowBaseView.render_template
            'scheduler_job',
            # airflow.www.views.AirflowBaseView.extra_args
            'macros',
        ]
        for key in keys_to_delete:
            del result[key]

        return result


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings.configure_orm()
        cls.session = settings.Session
        cls.app = application.create_app(testing=True)
        cls.appbuilder = cls.app.appbuilder  # pylint: disable=no-member
        cls.app.config['WTF_CSRF_ENABLED'] = False
        cls.app.jinja_env.undefined = jinja2.StrictUndefined

    @classmethod
    def tearDownClass(cls) -> None:
        clear_db_runs()

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

    @contextmanager
    def capture_templates(self) -> Generator[List[TemplateWithContext], None, None]:
        recorded = []

        def record(sender, template, context, **extra):  # pylint: disable=unused-argument
            recorded.append(TemplateWithContext(template, context))
        template_rendered.connect(record, self.app)  # type: ignore
        try:
            yield recorded
        finally:
            template_rendered.disconnect(record, self.app)  # type: ignore

        assert recorded, "Failed to catch the templates"

    @classmethod
    def clear_table(cls, model):
        with create_session() as session:
            session.query(model).delete()

    def check_content_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        self.assertEqual(resp_code, resp.status_code)
        if isinstance(text, list):
            for line in text:
                self.assertIn(line, resp_html)
        else:
            self.assertIn(text, resp_html)

    def check_content_not_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        self.assertEqual(resp_code, resp.status_code)
        if isinstance(text, list):
            for line in text:
                self.assertNotIn(line, resp_html)
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
    @conf_vars({("webserver", "base_url"): "http://localhost/test"})
    def setUpClass(cls):
        application.app = None
        application.appbuilder = None
        app = application.cached_app(config={'WTF_CSRF_ENABLED': False}, testing=True)
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

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.dagbag = models.DagBag(include_examples=True)
        DAG.bulk_sync_to_db(cls.dagbag.dags.values())

    def setUp(self):
        super().setUp()
        self.logout()
        self.login()
        clear_db_runs()
        self.prepare_dagruns()

    def prepare_dagruns(self):
        self.bash_dag = self.dagbag.dags['example_bash_operator']
        self.sub_dag = self.dagbag.dags['example_subdag_operator']
        self.xcom_dag = self.dagbag.dags['example_xcom']

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.xcom_dagrun = self.xcom_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

    def test_index(self):
        with assert_queries_count(37):
            resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('DAGs', resp)

    def test_doc_site_url(self):
        resp = self.client.get('/', follow_redirects=True)
        if "dev" in version.version:
            airflow_doc_site = "https://airflow.readthedocs.io/en/latest"
        else:
            airflow_doc_site = 'https://airflow.apache.org/docs/{}'.format(version.version)

        self.check_content_in_response(airflow_doc_site, resp)

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
        with self.capture_templates() as templates:
            resp = self.client.get('home', follow_redirects=True)
            self.check_content_in_response('DAGs', resp)
            val_state_color_mapping = 'const STATE_COLOR = {"failed": "red", ' \
                                      '"null": "lightblue", "queued": "gray", ' \
                                      '"removed": "lightgrey", "running": "lime", ' \
                                      '"scheduled": "tan", "shutdown": "blue", ' \
                                      '"skipped": "pink", "success": "green", ' \
                                      '"up_for_reschedule": "turquoise", ' \
                                      '"up_for_retry": "gold", "upstream_failed": "orange"};'
            self.check_content_in_response(val_state_color_mapping, resp)

        self.assertEqual(len(templates), 1)
        self.assertEqual(templates[0].name, 'airflow/dags.html')
        state_color_mapping = State.state_color.copy()
        state_color_mapping["null"] = state_color_mapping.pop(None)
        self.assertEqual(templates[0].local_context['state_color'], state_color_mapping)

    def test_users_list(self):
        resp = self.client.get('users/list', follow_redirects=True)
        self.check_content_in_response('List Users', resp)

    def test_roles_list(self):
        resp = self.client.get('roles/list', follow_redirects=True)
        self.check_content_in_response('List Roles', resp)

    def test_userstatschart_view(self):
        resp = self.client.get('userstatschartview/chart/', follow_redirects=True)
        self.check_content_in_response('User Statistics', resp)

    def test_permissions_list(self):
        resp = self.client.get('permissions/list/', follow_redirects=True)
        self.check_content_in_response('List Base Permissions', resp)

    def test_viewmenus_list(self):
        resp = self.client.get('viewmenus/list/', follow_redirects=True)
        self.check_content_in_response('List View Menus', resp)

    def test_permissionsviews_list(self):
        resp = self.client.get('permissionviews/list/', follow_redirects=True)
        self.check_content_in_response('List Permissions on Views/Menus', resp)

    def test_home_filter_tags(self):
        from airflow.www.views import FILTER_TAGS_COOKIE
        with self.client:
            self.client.get('home?tags=example&tags=data', follow_redirects=True)
            self.assertEqual('example,data', flask_session[FILTER_TAGS_COOKIE])

            self.client.get('home?reset_tags', follow_redirects=True)
            self.assertEqual(None, flask_session[FILTER_TAGS_COOKIE])

    def test_home_status_filter_cookie(self):
        from airflow.www.views import FILTER_STATUS_COOKIE
        with self.client:
            self.client.get('home', follow_redirects=True)
            self.assertEqual('all', flask_session[FILTER_STATUS_COOKIE])

            self.client.get('home?status=active', follow_redirects=True)
            self.assertEqual('active', flask_session[FILTER_STATUS_COOKIE])

            self.client.get('home?status=paused', follow_redirects=True)
            self.assertEqual('paused', flask_session[FILTER_STATUS_COOKIE])

            self.client.get('home?status=all', follow_redirects=True)
            self.assertEqual('all', flask_session[FILTER_STATUS_COOKIE])

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
        resp = self.client.post(url, follow_redirects=True)
        self.assertEqual(200, resp.status_code)

    def test_dag_stats(self):
        resp = self.client.post('dag_stats', follow_redirects=True)
        self.assertEqual(resp.status_code, 200)

    def test_task_stats(self):
        resp = self.client.post('task_stats', follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(set(list(resp.json.items())[0][1][0].keys()),
                         {'state', 'count'})

    @conf_vars({("webserver", "show_recent_stats_for_completed_runs"): "False"})
    def test_task_stats_only_noncompleted(self):
        resp = self.client.post('task_stats', follow_redirects=True)
        self.assertEqual(resp.status_code, 200)

    def test_dag_details(self):
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG details', resp)

    @parameterized.expand(["graph", "tree", "dag_details"])
    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_view_uses_existing_dagbag(self, endpoint, mock_get_dag):
        """
        Test that Graph, Tree & Dag Details View uses the DagBag already created in views.py
        instead of creating a new one.
        """
        mock_get_dag.return_value = DAG(dag_id='example_bash_operator')
        url = f'{endpoint}?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        mock_get_dag.assert_called_once_with('example_bash_operator')
        self.check_content_in_response('example_bash_operator', resp)

    @parameterized.expand([
        ("hello\nworld", "hello\\\\nworld"),
        ("hello'world", "hello\\\\u0027world"),
        ("<script>", "\\\\u003cscript\\\\u003e"),
    ])
    def test_escape_in_tree_view(self, test_str, seralized_test_str):
        dag = self.dagbag.dags['test_tree_view']
        dag.create_dagrun(
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            conf={"abc": test_str},
        )

        url = 'tree?dag_id=test_tree_view'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response(f'"conf":{{"abc":"{seralized_test_str}"}}', resp)

    def test_dag_details_trigger_origin_tree_view(self):
        dag = self.dagbag.dags['test_tree_view']
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        url = 'dag_details?dag_id=test_tree_view'
        resp = self.client.get(url, follow_redirects=True)
        params = {'dag_id': 'test_tree_view', 'origin': '/tree?dag_id=test_tree_view'}
        href = "/trigger?{}".format(html.escape(urllib.parse.urlencode(params)))
        self.check_content_in_response(href, resp)

    def test_dag_details_trigger_origin_graph_view(self):
        dag = self.dagbag.dags['test_graph_view']
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        url = 'dag_details?dag_id=test_graph_view'
        resp = self.client.get(url, follow_redirects=True)
        params = {'dag_id': 'test_graph_view', 'origin': '/graph?dag_id=test_graph_view'}
        href = "/trigger?{}".format(html.escape(urllib.parse.urlencode(params)))
        self.check_content_in_response(href, resp)

    def test_dag_details_subdag(self):
        url = 'dag_details?dag_id=example_subdag_operator.section-1'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG details', resp)

    def test_graph(self):
        url = 'graph?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_last_dagruns(self):
        resp = self.client.post('last_dagruns', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_last_dagruns_success_when_selecting_dags(self):
        resp = self.client.post('last_dagruns',
                                data={'dag_ids': ['example_subdag_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        stats = json.loads(resp.data.decode('utf-8'))
        self.assertNotIn('example_bash_operator', stats)
        self.assertIn('example_subdag_operator', stats)

        # Multiple
        resp = self.client.post('last_dagruns',
                                data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        stats = json.loads(resp.data.decode('utf-8'))
        self.assertIn('example_bash_operator', stats)
        self.assertIn('example_subdag_operator', stats)
        self.check_content_not_in_response('example_xcom', resp)

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
        self.check_content_not_in_response('Failed to load file', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_code_no_file(self):
        url = 'code?dag_id=example_bash_operator'
        mock_open_patch = mock.mock_open(read_data='')
        mock_open_patch.side_effect = FileNotFoundError
        with mock.patch('io.open', mock_open_patch):
            resp = self.client.get(url, follow_redirects=True)
            self.check_content_in_response('Failed to load file', resp)
            self.check_content_in_response('example_bash_operator', resp)

    @conf_vars({("core", "store_dag_code"): "True"})
    def test_code_from_db(self):
        from airflow.models.dagcode import DagCode
        dag = models.DagBag(include_examples=True).get_dag("example_bash_operator")
        DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url)
        self.check_content_not_in_response('Failed to load file', resp)
        self.check_content_in_response('example_bash_operator', resp)

    @conf_vars({("core", "store_dag_code"): "True"})
    def test_code_from_db_all_example_dags(self):
        from airflow.models.dagcode import DagCode
        dagbag = models.DagBag(include_examples=True)
        for dag in dagbag.dags.values():
            DagCode(dag.fileloc, DagCode._get_code_from_file(dag.fileloc)).sync_to_db()
        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url)
        self.check_content_not_in_response('Failed to load file', resp)
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

    def test_failed_dag_never_run(self):
        endpoint = "failed"
        dag_id = "example_bash_operator"
        form = dict(
            task_id="run_this_last",
            dag_id=dag_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        clear_db_runs()
        resp = self.client.post(endpoint, data=form, follow_redirects=True)
        self.check_content_in_response(
            f"Cannot make {endpoint}, seem that dag {dag_id} has never run", resp)

    def test_failed_flash_hint(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            confirmed="true",
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        resp = self.client.post("failed", data=form, follow_redirects=True)
        self.check_content_in_response("Marked failed on 1 task instances", resp)

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

    def test_success_dag_never_run(self):
        endpoint = "success"
        dag_id = "example_bash_operator"
        form = dict(
            task_id="run_this_last",
            dag_id=dag_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        clear_db_runs()
        resp = self.client.post('success', data=form, follow_redirects=True)
        self.check_content_in_response(
            f"Cannot make {endpoint}, seem that dag {dag_id} has never run", resp)

    def test_success_flash_hint(self):
        form = dict(
            task_id="run_this_last",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            confirmed="true",
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            origin="/graph?dag_id=example_bash_operator",
        )
        resp = self.client.post("success", data=form, follow_redirects=True)
        self.check_content_in_response("Marked success on 1 task instances", resp)

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

    @mock.patch('airflow.executors.executor_loader.ExecutorLoader.get_default_executor')
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

    @mock.patch('airflow.executors.executor_loader.ExecutorLoader.get_default_executor')
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

    def test_delete_dag_button_normal(self):
        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('/delete?dag_id=example_bash_operator', resp)
        self.check_content_in_response("return confirmDeleteDag(this, 'example_bash_operator')", resp)

    def test_delete_dag_button_for_dag_on_scheduler_only(self):
        # Test for JIRA AIRFLOW-3233 (PR 4069):
        # The delete-dag URL should be generated correctly for DAGs
        # that exist on the scheduler (DB) but not the webserver DagBag

        dag_id = 'example_bash_operator'
        test_dag_id = "non_existent_dag"

        DM = models.DagModel
        dag_query = self.session.query(DM).filter(DM.dag_id == dag_id)
        dag_query.first().tags = []  # To avoid "FOREIGN KEY constraint" error
        self.session.commit()

        dag_query.update({'dag_id': test_dag_id})
        self.session.commit()

        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('/delete?dag_id={}'.format(test_dag_id), resp)
        self.check_content_in_response("return confirmDeleteDag(this, '{}')".format(test_dag_id), resp)

        self.session.query(DM).filter(DM.dag_id == test_dag_id).update({'dag_id': dag_id})
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
    DAG_ID_REMOVED = 'removed_dag_for_testing_log_view'
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

        with conf_vars({('logging', 'logging_config_class'): 'airflow_local_settings.LOGGING_CONFIG'}):
            self.app = application.create_app(testing=True)
            self.appbuilder = self.app.appbuilder  # pylint: disable=no-member
            self.app.config['WTF_CSRF_ENABLED'] = False
            self.client = self.app.test_client()
            settings.configure_orm()
            self.login()

            from airflow.www.views import dagbag
            dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
            dag.sync_to_db()
            dag_removed = DAG(self.DAG_ID_REMOVED, start_date=self.DEFAULT_DATE)
            dag_removed.sync_to_db()
            dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
            with create_session() as session:
                self.ti = TaskInstance(
                    task=DummyOperator(task_id=self.TASK_ID, dag=dag),
                    execution_date=self.DEFAULT_DATE
                )
                self.ti.try_number = 1
                self.ti_removed_dag = TaskInstance(
                    task=DummyOperator(task_id=self.TASK_ID, dag=dag_removed),
                    execution_date=self.DEFAULT_DATE
                )
                self.ti_removed_dag.try_number = 1

                session.merge(self.ti)
                session.merge(self.ti_removed_dag)

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        self.clear_table(TaskInstance)

        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

        sys.path.remove(self.settings_folder)
        shutil.rmtree(self.settings_folder)
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

    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_get_logs_with_metadata_for_removed_dag(self, mock_read):
        mock_read.return_value = (['airflow log line'], [{'end_of_log': True}])
        url_template = "get_logs_with_metadata?dag_id={}&" \
                       "task_id={}&execution_date={}&" \
                       "try_number={}&metadata={}"
        url = url_template.format(self.DAG_ID_REMOVED, self.TASK_ID,
                                  quote_plus(self.DEFAULT_DATE.isoformat()), 1, json.dumps({}))
        response = self.client.get(
            url,
            data=dict(
                username='test',
                password='test'
            ),
            follow_redirects=True
        )

        self.assertIn('"message":', response.data.decode('utf-8'))
        self.assertIn('"metadata":', response.data.decode('utf-8'))
        self.assertIn('airflow log line', response.data.decode('utf-8'))
        self.assertEqual(200, response.status_code)


class TestVersionView(TestBase):
    def test_version(self):
        with self.capture_templates() as templates:
            resp = self.client.get('version', data=dict(
                username='test',
                password='test'
            ), follow_redirects=True)
            self.check_content_in_response('Version Info', resp)

        self.assertEqual(len(templates), 1)
        self.assertEqual(templates[0].name, 'airflow/version.html')
        self.assertEqual(templates[0].local_context, dict(
            airflow_version=version.version,
            git_version=mock.ANY,
            title='Version Info'
        ))


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
        self.runs = []

    def setup(self):
        from airflow.www.views import dagbag
        dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
        dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
        for run_data in self.RUNS_DATA:
            run = dag.create_dagrun(
                run_id=run_data[0],
                execution_date=run_data[1],
                state=State.SUCCESS,
                external_trigger=True
            )
            self.runs.append(run)

    def teardown(self):
        self.test.session.query(DagRun).filter(
            DagRun.dag_id == self.DAG_ID).delete()
        self.test.session.commit()
        self.test.session.close()

    def assert_base_date_and_num_runs(self, base_date, num_runs, data):
        self.test.assertNotIn('name="base_date" value="{}"'.format(base_date), data)
        self.test.assertNotIn('<option selected="" value="{num}">{num}</option>'.format(
            num=num_runs), data)

    def assert_run_is_not_in_dropdown(self, run, data):
        self.test.assertNotIn(run.execution_date.isoformat(), data)
        self.test.assertNotIn(run.run_id, data)

    def assert_run_is_in_dropdown_not_selected(self, run, data):
        self.test.assertIn('<option value="{}">{}</option>'.format(
            run.execution_date.isoformat(), run.run_id), data)

    def assert_run_is_selected(self, run, data):
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
        self.assert_run_is_selected(self.runs[0], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[3], data)

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
        self.assert_base_date_and_num_runs(
            self.runs[1].execution_date,
            conf.getint('webserver', 'default_dag_run_display_number'),
            data)
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[3], data)

    def test_with_base_date_and_num_runs_parameters_only(self):
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
        self.assert_base_date_and_num_runs(self.runs[1].execution_date, 2, data)
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_not_in_dropdown(self.runs[3], data)

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
        self.assert_base_date_and_num_runs(self.runs[1].execution_date, 42, data)
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_selected(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[3], data)

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
        self.assert_base_date_and_num_runs(self.runs[2].execution_date, 5, data)
        self.assert_run_is_not_in_dropdown(self.runs[0], data)
        self.assert_run_is_not_in_dropdown(self.runs[1], data)
        self.assert_run_is_in_dropdown_not_selected(self.runs[2], data)
        self.assert_run_is_selected(self.runs[3], data)


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
        self.tester.setup()

    def tearDown(self):
        self.tester.teardown()
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_dt_nr_dr_form_default_parameters(self):
        self.tester.test_with_default_parameters()

    def test_dt_nr_dr_form_with_execution_date_parameter_only(self):
        self.tester.test_with_execution_date_parameter_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_parmeters_only(self):
        self.tester.test_with_base_date_and_num_runs_parameters_only()

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
        self.tester.setup()

    def tearDown(self):
        self.tester.teardown()
        super().tearDown()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_dt_nr_dr_form_default_parameters(self):
        self.tester.test_with_default_parameters()

    def test_dt_nr_dr_form_with_execution_date_parameter_only(self):
        self.tester.test_with_execution_date_parameter_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_parmeters_only(self):
        self.tester.test_with_base_date_and_num_runs_parameters_only()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_outside(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_outside()

    def test_dt_nr_dr_form_with_base_date_and_num_runs_and_execution_date_within(self):
        self.tester.test_with_base_date_and_num_runs_and_execution_date_within()


class TestDagACLView(TestBase):
    """
    Test Airflow DAG acl
    """
    next_year = dt.now().year + 1
    default_date = timezone.datetime(next_year, 6, 1)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dagbag = models.DagBag(include_examples=True)
        DAG.bulk_sync_to_db(dagbag.dags.values())

    def prepare_dagruns(self):
        dagbag = models.DagBag(include_examples=True)
        self.bash_dag = dagbag.dags['example_bash_operator']
        self.sub_dag = dagbag.dags['example_subdag_operator']

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

    def setUp(self):
        super().setUp()
        clear_db_runs()
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

    def test_dag_autocomplete_success(self):
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.get(
            'dagmodel/autocomplete?query=example_bash',
            follow_redirects=False)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_dag_stats_success(self):
        self.logout()
        self.login()
        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.assertEqual(set(list(resp.json.items())[0][1][0].keys()),
                         {'state', 'count'})

    def test_dag_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_dag_stats_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_dag_stats_success_when_selecting_dags(self):
        resp = self.client.post('dag_stats',
                                data={'dag_ids': ['example_subdag_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        stats = json.loads(resp.data.decode('utf-8'))
        self.assertNotIn('example_bash_operator', stats)
        self.assertIn('example_subdag_operator', stats)

        # Multiple
        resp = self.client.post('dag_stats',
                                data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        stats = json.loads(resp.data.decode('utf-8'))
        self.assertIn('example_bash_operator', stats)
        self.assertIn('example_subdag_operator', stats)
        self.check_content_not_in_response('example_xcom', resp)

    def test_task_stats_success(self):
        self.logout()
        self.login()
        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_task_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_task_stats_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_task_stats_success_when_selecting_dags(self):
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')

        resp = self.client.post('task_stats',
                                data={'dag_ids': ['example_subdag_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        stats = json.loads(resp.data.decode('utf-8'))
        self.assertNotIn('example_bash_operator', stats)
        self.assertIn('example_subdag_operator', stats)

        # Multiple
        resp = self.client.post('task_stats',
                                data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        stats = json.loads(resp.data.decode('utf-8'))
        self.assertIn('example_bash_operator', stats)
        self.assertIn('example_subdag_operator', stats)
        self.check_content_not_in_response('example_xcom', resp)

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
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_blocked_success_for_all_dag_user(self):
        url = 'blocked'
        self.logout()
        self.login(username='all_dag_user',
                   password='all_dag_user')
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_blocked_success_when_selecting_dags(self):
        resp = self.client.post('blocked',
                                data={'dag_ids': ['example_subdag_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        blocked_dags = {blocked['dag_id'] for blocked in json.loads(resp.data.decode('utf-8'))}
        self.assertNotIn('example_bash_operator', blocked_dags)
        self.assertIn('example_subdag_operator', blocked_dags)

        # Multiple
        resp = self.client.post('blocked',
                                data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
                                follow_redirects=True)
        self.assertEqual(resp.status_code, 200)
        blocked_dags = {blocked['dag_id'] for blocked in json.loads(resp.data.decode('utf-8'))}
        self.assertIn('example_bash_operator', blocked_dags)
        self.assertIn('example_subdag_operator', blocked_dags)
        self.check_content_not_in_response('example_xcom', resp)

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


class TestRenderedView(TestBase):

    def setUp(self):
        super().setUp()
        self.default_date = datetime(2020, 3, 1)
        self.dag = DAG(
            "testdag",
            start_date=self.default_date,
            user_defined_filters={"hello": lambda name: f'Hello {name}'},
            user_defined_macros={"fullname": lambda fname, lname: f'{fname} {lname}'}
        )
        self.task1 = BashOperator(
            task_id='task1',
            bash_command='{{ task_instance_key_str }}',
            dag=self.dag
        )
        self.task2 = BashOperator(
            task_id='task2',
            bash_command='echo {{ fullname("Apache", "Airflow") | hello }}',
            dag=self.dag
        )
        SerializedDagModel.write_dag(self.dag)
        with create_session() as session:
            session.query(RTIF).delete()

    def tearDown(self) -> None:
        super().tearDown()
        with create_session() as session:
            session.query(RTIF).delete()

    @mock.patch('airflow.www.views.STORE_SERIALIZED_DAGS', True)
    @mock.patch('airflow.models.taskinstance.STORE_SERIALIZED_DAGS', True)
    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_rendered_view(self, get_dag_function):
        """
        Test that the Rendered View contains the values from RenderedTaskInstanceFields
        """
        get_dag_function.return_value = SerializedDagModel.get(self.dag.dag_id).dag

        self.assertEqual(self.task1.bash_command, '{{ task_instance_key_str }}')
        ti = TaskInstance(self.task1, self.default_date)

        with create_session() as session:
            session.add(RTIF(ti))

        url = ('rendered?task_id=task1&dag_id=testdag&execution_date={}'
               .format(self.percent_encode(self.default_date)))

        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response("testdag__task1__20200301", resp)

    @mock.patch('airflow.www.views.STORE_SERIALIZED_DAGS', True)
    @mock.patch('airflow.models.taskinstance.STORE_SERIALIZED_DAGS', True)
    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_rendered_view_for_unexecuted_tis(self, get_dag_function):
        """
        Test that the Rendered View is able to show rendered values
        even for TIs that have not yet executed
        """
        get_dag_function.return_value = SerializedDagModel.get(self.dag.dag_id).dag

        self.assertEqual(self.task1.bash_command, '{{ task_instance_key_str }}')

        url = ('rendered?task_id=task1&dag_id=task1&execution_date={}'
               .format(self.percent_encode(self.default_date)))

        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response("testdag__task1__20200301", resp)

    @mock.patch('airflow.www.views.STORE_SERIALIZED_DAGS', True)
    @mock.patch('airflow.models.taskinstance.STORE_SERIALIZED_DAGS', True)
    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_user_defined_filter_and_macros_raise_error(self, get_dag_function):
        """
        Test that the Rendered View is able to show rendered values
        even for TIs that have not yet executed
        """
        get_dag_function.return_value = SerializedDagModel.get(self.dag.dag_id).dag

        self.assertEqual(self.task2.bash_command,
                         'echo {{ fullname("Apache", "Airflow") | hello }}')

        url = ('rendered?task_id=task2&dag_id=testdag&execution_date={}'
               .format(self.percent_encode(self.default_date)))

        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response("echo Hello Apache Airflow", resp)
        self.check_content_in_response(
            "Webserver does not have access to User-defined Macros or Filters "
            "when Dag Serialization is enabled. Hence for the task that have not yet "
            "started running, please use &#39;airflow tasks render&#39; for debugging the "
            "rendering of template_fields.<br/><br/>OriginalError: no filter named &#39;hello&#39",
            resp
        )


@pytest.mark.quarantined
class TestTriggerDag(TestBase):

    def setUp(self):
        super().setUp()
        self.session = Session()
        models.DagBag().get_dag("example_bash_operator").sync_to_db(session=self.session)

    def test_trigger_dag_button_normal_exist(self):
        resp = self.client.get('/', follow_redirects=True)
        self.assertIn('/trigger?dag_id=example_bash_operator', resp.data.decode('utf-8'))
        self.assertIn("return confirmDeleteDag(this, 'example_bash_operator')", resp.data.decode('utf-8'))

    @pytest.mark.xfail(condition=using_mysql, reason="This test might be flaky on mysql")
    def test_trigger_dag_button(self):

        test_dag_id = "example_bash_operator"

        DR = models.DagRun
        self.session.query(DR).delete()
        self.session.commit()

        self.client.post('trigger?dag_id={}'.format(test_dag_id))

        run = self.session.query(DR).filter(DR.dag_id == test_dag_id).first()
        self.assertIsNotNone(run)
        self.assertIn(DagRunType.MANUAL.value, run.run_id)
        self.assertEqual(run.run_type, DagRunType.MANUAL.value)

    @pytest.mark.xfail(condition=using_mysql, reason="This test might be flaky on mysql")
    def test_trigger_dag_conf(self):

        test_dag_id = "example_bash_operator"
        conf_dict = {'string': 'Hello, World!'}

        DR = models.DagRun
        self.session.query(DR).delete()
        self.session.commit()

        self.client.post('trigger?dag_id={}'.format(test_dag_id), data={'conf': json.dumps(conf_dict)})

        run = self.session.query(DR).filter(DR.dag_id == test_dag_id).first()
        self.assertIsNotNone(run)
        self.assertIn(DagRunType.MANUAL.value, run.run_id)
        self.assertEqual(run.run_type, DagRunType.MANUAL.value)
        self.assertEqual(run.conf, conf_dict)

    def test_trigger_dag_conf_malformed(self):
        test_dag_id = "example_bash_operator"

        DR = models.DagRun
        self.session.query(DR).delete()
        self.session.commit()

        response = self.client.post('trigger?dag_id={}'.format(test_dag_id), data={'conf': '{"a": "b"'})
        self.check_content_in_response('Invalid JSON configuration', response)

        run = self.session.query(DR).filter(DR.dag_id == test_dag_id).first()
        self.assertIsNone(run)

    def test_trigger_dag_form(self):
        test_dag_id = "example_bash_operator"

        resp = self.client.get('trigger?dag_id={}'.format(test_dag_id))

        self.assertEqual(resp.status_code, 200)
        self.check_content_in_response('Trigger DAG: {}'.format(test_dag_id), resp)

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_trigger_endpoint_uses_existing_dagbag(self, mock_get_dag):
        """
        Test that Trigger Endpoint uses the DagBag already created in views.py
        instead of creating a new one.
        """
        mock_get_dag.return_value = DAG(dag_id='example_bash_operator')
        url = 'trigger?dag_id=example_bash_operator'
        resp = self.client.post(url, data={}, follow_redirects=True)
        mock_get_dag.assert_called_once_with('example_bash_operator')
        self.check_content_in_response('example_bash_operator', resp)


class TestExtraLinks(TestBase):
    def setUp(self):
        from tests.test_utils.mock_operators import Dummy3TestOperator
        from tests.test_utils.mock_operators import Dummy2TestOperator
        super().setUp()
        self.endpoint = "extra_links"
        self.default_date = datetime(2017, 1, 1)

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

        self.dag = DAG('dag', start_date=self.default_date)
        self.task = DummyTestOperator(task_id="some_dummy_task", dag=self.dag)
        self.task_2 = Dummy2TestOperator(task_id="some_dummy_task_2", dag=self.dag)
        self.task_3 = Dummy3TestOperator(task_id="some_dummy_task_3", dag=self.dag)

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_extra_links_works(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=foo-bar"
            .format(self.endpoint, self.dag.dag_id, self.task.task_id, self.default_date),
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
            .format(self.endpoint, self.dag.dag_id, self.task.task_id, self.default_date),
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
    def test_extra_link_in_gantt_view(self, get_dag_function):
        get_dag_function.return_value = self.dag

        exec_date = dates.days_ago(2)
        start_date = datetime(2020, 4, 10, 2, 0, 0)
        end_date = exec_date + timedelta(seconds=30)

        with create_session() as session:
            for task in self.dag.tasks:
                ti = TaskInstance(task=task, execution_date=exec_date, state="success")
                ti.start_date = start_date
                ti.end_date = end_date
                session.add(ti)

        url = 'gantt?dag_id={}&execution_date={}'.format(self.dag.dag_id, exec_date)
        resp = self.client.get(url, follow_redirects=True)

        self.check_content_in_response('"extraLinks":', resp)

        extra_links_grps = re.search(r'extraLinks\": \[(\".*?\")\]', resp.get_data(as_text=True))
        extra_links = extra_links_grps.group(0)
        self.assertIn('airflow', extra_links)
        self.assertIn('github', extra_links)

    @mock.patch('airflow.www.views.dagbag.get_dag')
    def test_operator_extra_link_override_global_extra_link(self, get_dag_function):
        get_dag_function.return_value = self.dag

        response = self.client.get(
            "{0}?dag_id={1}&task_id={2}&execution_date={3}&link_name=airflow".format(
                self.endpoint, self.dag.dag_id, self.task.task_id, self.default_date),
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
            .format(self.endpoint, self.dag.dag_id, self.task.task_id, self.default_date),
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
            .format(self.endpoint, self.dag.dag_id, self.task.task_id, self.default_date),
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
                self.endpoint, self.dag.dag_id, self.task_2.task_id, self.default_date),
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
                self.endpoint, self.dag.dag_id, self.task_2.task_id, self.default_date),
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
                self.endpoint, self.dag.dag_id, self.task_3.task_id, self.default_date),
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
                self.endpoint, self.dag.dag_id, self.task_3.task_id, self.default_date),
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

    def test_create_dagrun_execution_date_with_timezone_utc(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:04:03Z",
            "run_id": "test_create_dagrun",
        }
        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)

        dr = self.session.query(models.DagRun).one()

        self.assertEqual(dr.execution_date, dt(2018, 7, 6, 5, 4, 3, tzinfo=tz.utc))

    def test_create_dagrun_execution_date_with_timezone_edt(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:04:03-04:00",
            "run_id": "test_create_dagrun",
        }
        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)

        dr = self.session.query(models.DagRun).one()

        self.assertEqual(dr.execution_date, dt(2018, 7, 6, 5, 4, 3, tzinfo=tz(timedelta(hours=-4))))

    def test_create_dagrun_execution_date_with_timezone_pst(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:04:03-08:00",
            "run_id": "test_create_dagrun",
        }
        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)

        dr = self.session.query(models.DagRun).one()

        self.assertEqual(dr.execution_date, dt(2018, 7, 6, 5, 4, 3, tzinfo=tz(timedelta(hours=-8))))

    @conf_vars({("core", "default_timezone"): "America/Toronto"})
    def test_create_dagrun_execution_date_without_timezone_default_edt(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:04:03",
            "run_id": "test_create_dagrun",
        }
        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)

        dr = self.session.query(models.DagRun).one()

        self.assertEqual(dr.execution_date, dt(2018, 7, 6, 5, 4, 3, tzinfo=tz(timedelta(hours=-4))))

    def test_create_dagrun_execution_date_without_timezone_default_utc(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:04:03",
            "run_id": "test_create_dagrun",
        }
        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)

        dr = self.session.query(models.DagRun).one()

        self.assertEqual(dr.execution_date, dt(2018, 7, 6, 5, 4, 3, tzinfo=tz.utc))

    def test_create_dagrun_valid_conf(self):
        conf_value = dict(Valid=True)
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:05:03-02:00",
            "run_id": "test_create_dagrun_valid_conf",
            "conf": json.dumps(conf_value)
        }

        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('Added Row', resp)
        dr = self.session.query(models.DagRun).one()
        self.assertEqual(dr.conf, conf_value)

    def test_create_dagrun_invalid_conf(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:06:03",
            "run_id": "test_create_dagrun_invalid_conf",
            "conf": "INVALID: [JSON"
        }

        resp = self.client.post('/dagrun/add',
                                data=data,
                                follow_redirects=True)
        self.check_content_in_response('JSON Validation Error:', resp)
        dr = self.session.query(models.DagRun).all()
        self.assertFalse(dr)

    def test_list_dagrun_includes_conf(self):
        data = {
            "state": "running",
            "dag_id": "example_bash_operator",
            "execution_date": "2018-07-06 05:06:03",
            "run_id": "test_list_dagrun_includes_conf",
            "conf": '{"include": "me"}'
        }
        self.client.post('/dagrun/add', data=data, follow_redirects=True)
        dr = self.session.query(models.DagRun).one()
        self.assertEqual(dr.execution_date, timezone.convert_to_utc(datetime(2018, 7, 6, 5, 6, 3)))
        self.assertEqual(dr.conf, {"include": "me"})

        resp = self.client.get('/dagrun/list', follow_redirects=True)
        self.check_content_in_response("{'include': 'me'}", resp)


class TestDecorators(TestBase):
    EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dagbag = models.DagBag(include_examples=True)
        DAG.bulk_sync_to_db(dagbag.dags.values())

    def setUp(self):
        super().setUp()
        self.logout()
        self.login()
        clear_db_runs()
        self.prepare_dagruns()

    def prepare_dagruns(self):
        dagbag = models.DagBag(include_examples=True)
        self.bash_dag = dagbag.dags['example_bash_operator']
        self.sub_dag = dagbag.dags['example_subdag_operator']
        self.xcom_dag = dagbag.dags['example_xcom']

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

        self.xcom_dagrun = self.xcom_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
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
