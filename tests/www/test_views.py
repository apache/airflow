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
import json
import re
import unittest
import urllib
from contextlib import contextmanager
from datetime import datetime as dt
from typing import Any, Dict, Generator, List, NamedTuple
from unittest import mock
from unittest.mock import PropertyMock

import jinja2
from flask import template_rendered
from parameterized import parameterized

from airflow import models, settings
from airflow.executors.celery_executor import CeleryExecutor
from airflow.models import DAG
from airflow.security import permissions
from airflow.ti_deps.dependencies_states import QUEUEABLE_STATES, RUNNABLE_STATES
from airflow.utils import dates, timezone
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.www import app as application
from airflow.www.extensions import init_views
from airflow.www.extensions.init_appbuilder_links import init_appbuilder_links
from tests.test_utils import api_connexion_utils
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


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
            'state_color_mapping',
            'airflow_version',
            'git_version',
            'k8s_or_k8scelery_executor',
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
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_appbuilder",
            "init_appbuilder_views",
            "init_flash_views",
            "init_jinja_globals",
        ]
    )
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
        api_connexion_utils.delete_roles(cls.app)

    def setUp(self):
        self.client = self.app.test_client()
        self.login()

    def login(self, username='test', password='test'):
        with mock.patch('flask_appbuilder.security.manager.check_password_hash') as set_mock:
            set_mock.return_value = True
            if username == 'test' and not self.appbuilder.sm.find_user(username='test'):
                self.appbuilder.sm.add_user(
                    username='test',
                    first_name='test',
                    last_name='test',
                    email='test@fab.org',
                    role=self.appbuilder.sm.find_role('Admin'),
                    password='test',
                )
            if username == 'test_user' and not self.appbuilder.sm.find_user(username='test_user'):
                self.appbuilder.sm.add_user(
                    username='test_user',
                    first_name='test_user',
                    last_name='test_user',
                    email='test_user@fab.org',
                    role=self.appbuilder.sm.find_role('User'),
                    password='test_user',
                )

            if username == 'test_viewer' and not self.appbuilder.sm.find_user(username='test_viewer'):
                self.appbuilder.sm.add_user(
                    username='test_viewer',
                    first_name='test_viewer',
                    last_name='test_viewer',
                    email='test_viewer@fab.org',
                    role=self.appbuilder.sm.find_role('Viewer'),
                    password='test_viewer',
                )

            return self.client.post('/login/', data={"username": username, "password": password})

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
        assert resp_code == resp.status_code
        if isinstance(text, list):
            for line in text:
                assert line in resp_html
        else:
            assert text in resp_html

    def check_content_not_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        assert resp_code == resp.status_code
        if isinstance(text, list):
            for line in text:
                assert line not in resp_html
        else:
            assert text not in resp_html

    @staticmethod
    def percent_encode(obj):
        return urllib.parse.quote_plus(str(obj))

    def create_user_and_login(self, username, role_name, perms):
        self.logout()
        api_connexion_utils.create_user(
            self.app,
            username=username,
            role_name=role_name,
            permissions=perms,
        )
        self.login(username=username, password=username)


class TestAirflowBaseViews(TestBase):
    EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        models.DagBag(include_examples=True).sync_to_db()
        cls.dagbag = models.DagBag(include_examples=True, read_dags_from_db=True)
        cls.app.dag_bag = cls.dagbag
        with cls.app.app_context():
            init_views.init_api_connexion(cls.app)
            init_views.init_plugins(cls.app)
            init_appbuilder_links(cls.app)

    def setUp(self):
        super().setUp()
        self.logout()
        self.login()
        clear_db_runs()
        self.prepare_dagruns()

    def _delete_role_if_exists(self, role_name):
        if self.appbuilder.sm.find_role(role_name):
            self.appbuilder.sm.delete_role(role_name)

    def prepare_dagruns(self):
        self.bash_dag = self.dagbag.get_dag('example_bash_operator')
        self.sub_dag = self.dagbag.get_dag('example_subdag_operator')
        self.xcom_dag = self.dagbag.get_dag('example_xcom')

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        self.xcom_dagrun = self.xcom_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

    def test_task(self):
        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_xcom(self):
        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_xcom_list_view_title(self):
        resp = self.client.get('xcom/list', follow_redirects=True)
        self.check_content_in_response('List XComs', resp)

    def test_rendered_template(self):
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_rendered_k8s(self):
        url = 'rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        with mock.patch.object(settings, "IS_K8S_OR_K8SCELERY_EXECUTOR", True):
            resp = self.client.get(url, follow_redirects=True)
            self.check_content_in_response('K8s Pod Spec', resp)

    @conf_vars({('core', 'executor'): 'LocalExecutor'})
    def test_rendered_k8s_without_k8s(self):
        url = 'rendered-k8s?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)
        )
        resp = self.client.get(url, follow_redirects=True)
        assert 404 == resp.status_code

    def test_dag_details(self):
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG Details', resp)

    @parameterized.expand(["graph", "tree", "dag_details"])
    def test_view_uses_existing_dagbag(self, endpoint):
        """
        Test that Graph, Tree & Dag Details View uses the DagBag already created in views.py
        instead of creating a new one.
        """
        url = f'{endpoint}?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    @parameterized.expand(
        [
            ("hello\nworld", r'\"conf\":{\"abc\":\"hello\\nworld\"}'),
            ("hello'world", r'\"conf\":{\"abc\":\"hello\\u0027world\"}'),
            ("<script>", r'\"conf\":{\"abc\":\"\\u003cscript\\u003e\"}'),
            ("\"", r'\"conf\":{\"abc\":\"\\\"\"}'),
        ]
    )
    def test_escape_in_tree_view(self, test_str, expected_text):
        dag = self.dagbag.get_dag('test_tree_view')
        dag.create_dagrun(
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            conf={"abc": test_str},
        )

        url = 'tree?dag_id=test_tree_view'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response(expected_text, resp)

    def test_dag_details_trigger_origin_tree_view(self):
        dag = self.dagbag.get_dag('test_tree_view')
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        url = 'dag_details?dag_id=test_tree_view'
        resp = self.client.get(url, follow_redirects=True)
        params = {'dag_id': 'test_tree_view', 'origin': '/tree?dag_id=test_tree_view'}
        href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
        self.check_content_in_response(href, resp)

    def test_dag_details_trigger_origin_graph_view(self):
        dag = self.dagbag.get_dag('test_graph_view')
        dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        url = 'dag_details?dag_id=test_graph_view'
        resp = self.client.get(url, follow_redirects=True)
        params = {'dag_id': 'test_graph_view', 'origin': '/graph?dag_id=test_graph_view'}
        href = f"/trigger?{html.escape(urllib.parse.urlencode(params))}"
        self.check_content_in_response(href, resp)

    def test_dag_details_subdag(self):
        url = 'dag_details?dag_id=example_subdag_operator.section-1'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG Details', resp)

    def test_graph(self):
        url = 'graph?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_last_dagruns(self):
        resp = self.client.post('last_dagruns', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_last_dagruns_success_when_selecting_dags(self):
        resp = self.client.post(
            'last_dagruns', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' not in stats
        assert 'example_subdag_operator' in stats

        # Multiple
        resp = self.client.post(
            'last_dagruns',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' in stats
        assert 'example_subdag_operator' in stats
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
        with mock.patch('builtins.open', mock_open_patch), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", False
        ):
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
        self.check_content_in_response(f"Cannot make {endpoint}, seem that dag {dag_id} has never run", resp)

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
        self.check_content_in_response(f"Cannot make {endpoint}, seem that dag {dag_id} has never run", resp)

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
            self.session.query(models.TaskInstance).filter(models.TaskInstance.task_id == task_id).update(
                {'state': state, 'end_date': timezone.utcnow()}
            )
            self.session.commit()

            form = dict(
                task_id=task_id,
                dag_id="example_bash_operator",
                ignore_all_deps="false",
                ignore_ti_state="false",
                execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
                origin='/home',
            )
            resp = self.client.post('run', data=form, follow_redirects=True)

            self.check_content_in_response('', resp)

            msg = (
                f"Task is in the &#39;{state}&#39; state which is not a valid state for execution. "
                + "The task must be cleared in order to be run"
            )
            assert not re.search(msg, resp.get_data(as_text=True))

    @mock.patch('airflow.executors.executor_loader.ExecutorLoader.get_default_executor')
    def test_run_with_not_runnable_states(self, get_default_executor_function):
        get_default_executor_function.return_value = CeleryExecutor()

        task_id = 'runme_0'

        for state in QUEUEABLE_STATES:
            assert state not in RUNNABLE_STATES

            self.session.query(models.TaskInstance).filter(models.TaskInstance.task_id == task_id).update(
                {'state': state, 'end_date': timezone.utcnow()}
            )
            self.session.commit()

            form = dict(
                task_id=task_id,
                dag_id="example_bash_operator",
                ignore_all_deps="false",
                ignore_ti_state="false",
                execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
                origin='/home',
            )
            resp = self.client.post('run', data=form, follow_redirects=True)

            self.check_content_in_response('', resp)

            msg = (
                f"Task is in the &#39;{state}&#39; state which is not a valid state for execution. "
                + "The task must be cleared in order to be run"
            )
            assert re.search(msg, resp.get_data(as_text=True))

    def test_refresh(self):
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('', resp, resp_code=302)

    def test_refresh_all(self):
        with mock.patch.object(self.app.dag_bag, 'collect_dags_from_db') as collect_dags_from_db:
            resp = self.client.post("/refresh_all", follow_redirects=True)
            self.check_content_in_response('', resp)
            collect_dags_from_db.assert_called_once_with()

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

        DM = models.DagModel  # pylint: disable=invalid-name
        dag_query = self.session.query(DM).filter(DM.dag_id == dag_id)
        dag_query.first().tags = []  # To avoid "FOREIGN KEY constraint" error
        self.session.commit()

        dag_query.update({'dag_id': test_dag_id})
        self.session.commit()

        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response(f'/delete?dag_id={test_dag_id}', resp)
        self.check_content_in_response(f"return confirmDeleteDag(this, '{test_dag_id}')", resp)

        self.session.query(DM).filter(DM.dag_id == test_dag_id).update({'dag_id': dag_id})
        self.session.commit()

    @parameterized.expand(["graph", "tree"])
    def test_show_external_log_redirect_link_with_local_log_handler(self, endpoint):
        """Do not show external links if log handler is local."""
        url = f'{endpoint}?dag_id=example_bash_operator'
        with self.capture_templates() as templates:
            self.client.get(url, follow_redirects=True)
            ctx = templates[0].local_context
            assert not ctx['show_external_log_redirect']
            assert ctx['external_log_name'] is None

    @parameterized.expand(["graph", "tree"])
    @mock.patch('airflow.utils.log.log_reader.TaskLogReader.log_handler', new_callable=PropertyMock)
    def test_show_external_log_redirect_link_with_external_log_handler(self, endpoint, mock_log_handler):
        """Show external links if log handler is external."""

        class ExternalHandler(ExternalLoggingMixin):
            LOG_NAME = 'ExternalLog'

            @property
            def log_name(self):
                return self.LOG_NAME

        mock_log_handler.return_value = ExternalHandler()

        url = f'{endpoint}?dag_id=example_bash_operator'
        with self.capture_templates() as templates:
            self.client.get(url, follow_redirects=True)
            ctx = templates[0].local_context
            assert ctx['show_external_log_redirect']
            assert ctx['external_log_name'] == ExternalHandler.LOG_NAME


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
        DAG.bulk_write_to_db(dagbag.dags.values())
        for username in ['all_dag_user', 'dag_read_only', 'dag_faker', 'dag_tester']:
            user = cls.appbuilder.sm.find_user(username=username)
            if user:
                cls.appbuilder.sm.del_register_user(user)

    def prepare_dagruns(self):
        dagbag = models.DagBag(include_examples=True, read_dags_from_db=True)
        self.bash_dag = dagbag.get_dag("example_bash_operator")
        self.sub_dag = dagbag.get_dag("example_subdag_operator")

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        self.sub_dagrun = self.sub_dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=self.default_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
        )

    def setUp(self):
        super().setUp()
        clear_db_runs()
        self.prepare_dagruns()
        self.logout()

    def login(self, username='dag_tester', password='dag_test'):
        dag_tester_role = self.appbuilder.sm.add_role('dag_acl_tester')
        if username == 'dag_tester' and not self.appbuilder.sm.find_user(username='dag_tester'):
            self.appbuilder.sm.add_user(
                username='dag_tester',
                first_name='dag_test',
                last_name='dag_test',
                email='dag_test@fab.org',
                role=dag_tester_role,
                password='dag_test',
            )

        # create an user without permission
        dag_no_role = self.appbuilder.sm.add_role('dag_acl_faker')
        if username == 'dag_faker' and not self.appbuilder.sm.find_user(username='dag_faker'):
            self.appbuilder.sm.add_user(
                username='dag_faker',
                first_name='dag_faker',
                last_name='dag_faker',
                email='dag_fake@fab.org',
                role=dag_no_role,
                password='dag_faker',
            )

        # create an user with only read permission
        dag_read_only_role = self.appbuilder.sm.add_role('dag_acl_read_only')
        if username == 'dag_read_only' and not self.appbuilder.sm.find_user(username='dag_read_only'):
            self.appbuilder.sm.add_user(
                username='dag_read_only',
                first_name='dag_read_only',
                last_name='dag_read_only',
                email='dag_read_only@fab.org',
                role=dag_read_only_role,
                password='dag_read_only',
            )

        # create an user that has all dag access
        all_dag_role = self.appbuilder.sm.add_role('all_dag_role')
        if username == 'all_dag_user' and not self.appbuilder.sm.find_user(username='all_dag_user'):
            self.appbuilder.sm.add_user(
                username='all_dag_user',
                first_name='all_dag_user',
                last_name='all_dag_user',
                email='all_dag_user@fab.org',
                role=all_dag_role,
                password='all_dag_user',
            )

        return super().login(username, password)

    def add_permission_for_role(self):
        self.logout()
        self.login(username='test', password='test')
        website_permission = self.appbuilder.sm.find_permission_view_menu(
            permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE
        )

        dag_tester_role = self.appbuilder.sm.find_role('dag_acl_tester')
        edit_perm_on_dag = self.appbuilder.sm.find_permission_view_menu(
            permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'
        )
        self.appbuilder.sm.add_permission_role(dag_tester_role, edit_perm_on_dag)
        read_perm_on_dag = self.appbuilder.sm.find_permission_view_menu(
            permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'
        )
        self.appbuilder.sm.add_permission_role(dag_tester_role, read_perm_on_dag)
        self.appbuilder.sm.add_permission_role(dag_tester_role, website_permission)

        all_dag_role = self.appbuilder.sm.find_role('all_dag_role')
        edit_perm_on_all_dag = self.appbuilder.sm.find_permission_view_menu(
            permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG
        )
        self.appbuilder.sm.add_permission_role(all_dag_role, edit_perm_on_all_dag)
        read_perm_on_all_dag = self.appbuilder.sm.find_permission_view_menu(
            permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG
        )
        self.appbuilder.sm.add_permission_role(all_dag_role, read_perm_on_all_dag)
        read_perm_on_task_instance = self.appbuilder.sm.find_permission_view_menu(
            permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE
        )
        self.appbuilder.sm.add_permission_role(all_dag_role, read_perm_on_task_instance)
        self.appbuilder.sm.add_permission_role(all_dag_role, website_permission)

        role_user = self.appbuilder.sm.find_role('User')
        self.appbuilder.sm.add_permission_role(role_user, read_perm_on_all_dag)
        self.appbuilder.sm.add_permission_role(role_user, edit_perm_on_all_dag)
        self.appbuilder.sm.add_permission_role(role_user, website_permission)

        read_only_perm_on_dag = self.appbuilder.sm.find_permission_view_menu(
            permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'
        )
        dag_read_only_role = self.appbuilder.sm.find_role('dag_acl_read_only')
        self.appbuilder.sm.add_permission_role(dag_read_only_role, read_only_perm_on_dag)
        self.appbuilder.sm.add_permission_role(dag_read_only_role, website_permission)

        dag_acl_faker_role = self.appbuilder.sm.find_role('dag_acl_faker')
        self.appbuilder.sm.add_permission_role(dag_acl_faker_role, website_permission)

    def test_permission_exist(self):
        self.create_user_and_login(
            username='permission_exist_user',
            role_name='permission_exist_role',
            perms=[
                (permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'),
                (permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'),
            ],
        )

        test_view_menu = self.appbuilder.sm.find_view_menu('DAG:example_bash_operator')
        perms_views = self.appbuilder.sm.find_permissions_view_menu(test_view_menu)
        assert len(perms_views) == 2

        perms = [str(perm) for perm in perms_views]
        expected_perms = [
            'can read on DAG:example_bash_operator',
            'can edit on DAG:example_bash_operator',
        ]
        for perm in expected_perms:
            assert perm in perms

    def test_role_permission_associate(self):
        self.create_user_and_login(
            username='role_permission_associate_user',
            role_name='role_permission_associate_role',
            perms=[
                (permissions.ACTION_CAN_EDIT, 'DAG:example_bash_operator'),
                (permissions.ACTION_CAN_READ, 'DAG:example_bash_operator'),
            ],
        )

        test_role = self.appbuilder.sm.find_role('role_permission_associate_role')
        perms = {str(perm) for perm in test_role.permissions}
        assert 'can edit on DAG:example_bash_operator' in perms
        assert 'can read on DAG:example_bash_operator' in perms

    def test_index_success(self):
        self.create_user_and_login(
            username='index_success_user',
            role_name='index_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.get('/', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_index_failure(self):
        self.logout()
        self.login()
        resp = self.client.get('/', follow_redirects=True)
        # The user can only access/view example_bash_operator dag.
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_index_for_all_dag_user(self):
        self.create_user_and_login(
            username='index_for_all_dag_user',
            role_name='index_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.get('/', follow_redirects=True)
        # The all dag user can access/view all dags.
        self.check_content_in_response('example_subdag_operator', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_dag_autocomplete_success(self):
        self.login(username='all_dag_user', password='all_dag_user')
        resp = self.client.get('dagmodel/autocomplete?query=example_bash', follow_redirects=False)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_dag_stats_success(self):
        self.create_user_and_login(
            username='dag_stats_success_user',
            role_name='dag_stats_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        assert set(list(resp.json.items())[0][1][0].keys()) == {'state', 'count'}

    def test_dag_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_dag_stats_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='dag_stats_success_for_all_dag_user',
            role_name='dag_stats_success_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('dag_stats', follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)
        self.check_content_in_response('example_bash_operator', resp)

    def test_dag_stats_success_when_selecting_dags(self):
        self.add_permission_for_role()
        resp = self.client.post(
            'dag_stats', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' not in stats
        assert 'example_subdag_operator' in stats

        # Multiple
        resp = self.client.post(
            'dag_stats',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' in stats
        assert 'example_subdag_operator' in stats
        self.check_content_not_in_response('example_xcom', resp)

    def test_task_stats_success(self):
        self.create_user_and_login(
            username='task_stats_success_user',
            role_name='task_stats_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_task_stats_failure(self):
        self.logout()
        self.login()
        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_not_in_response('example_subdag_operator', resp)

    def test_task_stats_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='task_stats_success_for_all_dag_user',
            role_name='task_stats_success_for_all_dag_user_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.post('task_stats', follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_task_stats_success_when_selecting_dags(self):
        self.logout()
        username = 'task_stats_success_when_selecting_dags_user'
        self.create_user_and_login(
            username=username,
            role_name='task_stats_success_when_selecting_dags_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        self.login(username=username, password=username)

        resp = self.client.post(
            'task_stats', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' not in stats
        assert 'example_subdag_operator' in stats

        # Multiple
        resp = self.client.post(
            'task_stats',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )
        stats = json.loads(resp.data.decode('utf-8'))
        assert 'example_bash_operator' in stats
        assert 'example_subdag_operator' in stats
        self.check_content_not_in_response('example_xcom', resp)

    def test_code_success(self):
        self.create_user_and_login(
            username='code_success_user',
            role_name='code_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_code_failure(self):
        self.create_user_and_login(
            username='code_failure_user',
            role_name='code_failure_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_code_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='code_success_for_all_dag_user',
            role_name='code_success_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'code?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

        url = 'code?dag_id=example_subdag_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_dag_details_success(self):
        self.create_user_and_login(
            username='dag_details_success_user',
            role_name='dag_details_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('DAG Details', resp)

    def test_dag_details_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('DAG Details', resp)

    def test_dag_details_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='dag_details_success_for_all_dag_user',
            role_name='dag_details_success_for_all_dag_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'dag_details?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

        url = 'dag_details?dag_id=example_subdag_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_rendered_template_success(self):
        self.logout()
        username = 'rendered_success_user'
        self.create_user_and_login(
            username=username,
            role_name='rendered_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        self.login(username=username, password=username)
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_rendered_template_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Rendered Template', resp)

    def test_rendered_template_success_for_all_dag_user(self):
        self.logout()
        self.login(username='all_dag_user', password='all_dag_user')
        url = 'rendered-templates?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Rendered Template', resp)

    def test_task_success(self):
        self.create_user_and_login(
            username='task_success_user',
            role_name='task_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_task_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Task Instance Details', resp)

    def test_task_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='task_success_for_all_dag_user',
            role_name='task_success_for_all_dag_user_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'task?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Task Instance Details', resp)

    def test_xcom_success(self):
        self.logout()
        username = 'xcom_success_user'
        self.create_user_and_login(
            username=username,
            role_name='xcom_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        self.login(username=username, password=username)

        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('XCom', resp)

    def test_xcom_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('XCom', resp)

    def test_xcom_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='xcom_success_for_all_dag_user_user',
            role_name='xcom_success_for_all_dag_user_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'xcom?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
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
        self.login(username='all_dag_user', password='all_dag_user')
        form = dict(
            task_id="runme_0",
            dag_id="example_bash_operator",
            ignore_all_deps="false",
            ignore_ti_state="true",
            execution_date=self.default_date,
        )
        resp = self.client.post('run', data=form)
        self.check_content_in_response('', resp, resp_code=302)

    def test_blocked_success(self):
        self.create_user_and_login(
            username='blocked_success_user',
            role_name='blocked_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        url = 'blocked'

        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_blocked_success_for_all_dag_user(self):
        self.create_user_and_login(
            username='block_success_user',
            role_name='block_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'blocked'
        resp = self.client.post(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)
        self.check_content_in_response('example_subdag_operator', resp)

    def test_blocked_success_when_selecting_dags(self):
        self.add_permission_for_role()
        resp = self.client.post(
            'blocked', data={'dag_ids': ['example_subdag_operator']}, follow_redirects=True
        )
        assert resp.status_code == 200
        blocked_dags = {blocked['dag_id'] for blocked in json.loads(resp.data.decode('utf-8'))}
        assert 'example_bash_operator' not in blocked_dags
        assert 'example_subdag_operator' in blocked_dags

        # Multiple
        resp = self.client.post(
            'blocked',
            data={'dag_ids': ['example_subdag_operator', 'example_bash_operator']},
            follow_redirects=True,
        )

        blocked_dags = {blocked['dag_id'] for blocked in json.loads(resp.data.decode('utf-8'))}
        assert 'example_bash_operator' in blocked_dags
        assert 'example_subdag_operator' in blocked_dags
        self.check_content_not_in_response('example_xcom', resp)

    def test_failed_success(self):
        self.create_user_and_login(
            username='failed_success_user',
            role_name='failed_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

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
        self.check_content_in_response('example_bash_operator', resp)

    def test_duration_success(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        self.create_user_and_login(
            username='duration_success_user',
            role_name='duration_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_duration_failure(self):
        url = 'duration?days=30&dag_id=example_bash_operator'
        self.logout()
        # login as an user without permissions
        self.login(username='dag_faker', password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_tries_success(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        self.create_user_and_login(
            username='tries_success_user',
            role_name='tries_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_tries_failure(self):
        url = 'tries?days=30&dag_id=example_bash_operator'
        self.logout()
        # login as an user without permissions
        self.login(username='dag_faker', password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_landing_times_success(self):
        self.create_user_and_login(
            username='landing_times_success_user',
            role_name='landing_times_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'landing_times?days=30&dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_landing_times_failure(self):
        url = 'landing_times?days=30&dag_id=example_bash_operator'
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
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
        self.create_user_and_login(
            username='gantt_success_user',
            role_name='gantt_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('example_bash_operator', resp)

    def test_gantt_failure(self):
        url = 'gantt?dag_id=example_bash_operator'
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('example_bash_operator', resp)

    def test_success_fail_for_read_only_task_instance_access(self):
        # success endpoint need can_edit, which read only role can not access
        self.create_user_and_login(
            username='task_instance_read_user',
            role_name='task_instance_read_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            ],
        )

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
        # tree view only allows can_read, which read only role could access
        self.create_user_and_login(
            username='tree_success_for_read_only_role_user',
            role_name='tree_success_for_read_only_role_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_log_success(self):
        self.create_user_and_login(
            username='log_success_user',
            role_name='log_success_role',
            perms=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        url = 'log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Log by attempts', resp)
        url = (
            'get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}&try_number=1&metadata=null'.format(self.percent_encode(self.default_date))
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('"message":', resp)
        self.check_content_in_response('"metadata":', resp)

    def test_log_failure(self):
        self.logout()
        self.login(username='dag_faker', password='dag_faker')
        url = 'log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('Log by attempts', resp)
        url = (
            'get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}&try_number=1&metadata=null'.format(self.percent_encode(self.default_date))
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_not_in_response('"message":', resp)
        self.check_content_not_in_response('"metadata":', resp)

    def test_log_success_for_user(self):
        self.logout()
        self.login(username='test_user', password='test_user')
        url = 'log?task_id=runme_0&dag_id=example_bash_operator&execution_date={}'.format(
            self.percent_encode(self.default_date)
        )

        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('Log by attempts', resp)
        url = (
            'get_logs_with_metadata?task_id=runme_0&dag_id=example_bash_operator&'
            'execution_date={}&try_number=1&metadata=null'.format(self.percent_encode(self.default_date))
        )
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('"message":', resp)
        self.check_content_in_response('"metadata":', resp)

    def test_tree_view_for_viewer(self):
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        url = 'tree?dag_id=example_bash_operator'
        resp = self.client.get(url, follow_redirects=True)
        self.check_content_in_response('runme_1', resp)

    def test_refresh_failure_for_viewer(self):
        # viewer role can't refresh
        self.logout()
        self.login(username='test_viewer', password='test_viewer')
        resp = self.client.post('refresh?dag_id=example_bash_operator')
        self.check_content_in_response('Redirecting', resp, resp_code=302)
