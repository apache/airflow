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
import mock
import os
import shutil
import sys
import tempfile
import unittest
import urllib

from flask._compat import PY2
from flask_appbuilder.security.sqla.models import User as ab_user
from urllib.parse import quote_plus
from werkzeug.test import Client

from airflow import configuration as conf
from airflow import models
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.settings import Session
from airflow.utils import dates, timezone
from airflow.utils.state import State
from airflow.utils.timezone import datetime
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
    EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)
    run_id = "test_{}".format(models.DagRun.id_for_date(EXAMPLE_DAG_DEFAULT_DATE))

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
        resp = self.client.get('health')
        self.check_content_in_response('The server is healthy!', resp)

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
               .format(self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)))
        resp = self.client.get(url)
        self.check_content_in_response('Wait a minute', resp)

    def test_clear(self):
        url = ('clear?task_id=runme_1&dag_id=example_bash_operator&'
               'execution_date={}&upstream=false&downstream=false&future=false&past=false'
               .format(self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)))
        resp = self.client.get(url)
        self.check_content_in_response(['example_bash_operator', 'Wait a minute'], resp)

    def test_run(self):
        url = ('run?task_id=runme_0&dag_id=example_bash_operator&ignore_all_deps=false&'
               'ignore_ti_state=true&execution_date={}'
               .format(self.percent_encode(self.EXAMPLE_DAG_DEFAULT_DATE)))
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


class TestLogView(TestBase):
    DAG_ID = 'dag_for_testing_log_view'
    TASK_ID = 'task_for_testing_log_view'
    DEFAULT_DATE = timezone.datetime(2017, 9, 1)
    ENDPOINT = 'log?dag_id={dag_id}&task_id={task_id}&' \
               'execution_date={execution_date}'.format(dag_id=DAG_ID,
                                                        task_id=TASK_ID,
                                                        execution_date=DEFAULT_DATE)

    def setUp(self):
        conf.load_test_config()

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

        self.app, self.appbuilder = application.create_app(testing=True)
        self.app.config['WTF_CSRF_ENABLED'] = False
        self.client = self.app.test_client()
        self.login()
        self.session = Session()

        from airflow.www_rbac.views import dagbag
        dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
        task = DummyOperator(task_id=self.TASK_ID, dag=dag)
        dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
        ti = TaskInstance(task=task, execution_date=self.DEFAULT_DATE)
        ti.try_number = 1
        self.session.merge(ti)
        self.session.commit()

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        self.clear_table(TaskInstance)

        shutil.rmtree(self.settings_folder)
        conf.set('core', 'logging_config_class', '')

        self.logout()
        super(TestLogView, self).tearDown()

    def test_get_file_task_log(self):
        response = self.client.get(
            TestLogView.ENDPOINT,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn('Log by attempts',
                      response.data.decode('utf-8'))

    def test_get_logs_with_metadata(self):
        url_template = "get_logs_with_metadata?dag_id={}&" \
                       "task_id={}&execution_date={}&" \
                       "try_number={}&metadata={}"
        response = \
            self.client.get(url_template.format(self.DAG_ID,
                                                self.TASK_ID,
                                                quote_plus(self.DEFAULT_DATE.isoformat()),
                                                1,
                                                json.dumps({})), follow_redirects=True)

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
                                                1), follow_redirects=True)

        self.assertIn('"message":', response.data.decode('utf-8'))
        self.assertIn('"metadata":', response.data.decode('utf-8'))
        self.assertIn('Log for testing.', response.data.decode('utf-8'))
        self.assertEqual(200, response.status_code)


class TestVersionView(TestBase):
    def test_version(self):
        resp = self.client.get('version', follow_redirects=True)
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
        from airflow.www_rbac.views import dagbag
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
            self.endpoint
        )
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
                self.runs[1].execution_date.isoformat())
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
                self.runs[1].execution_date.isoformat())
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
                self.runs[0].execution_date.isoformat())
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
                self.runs[3].execution_date.isoformat())
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
        super(TestGraphView, cls).setUpClass()

    def setUp(self):
        super(TestGraphView, self).setUp()
        self.tester = ViewWithDateTimeAndNumRunsAndDagRunsFormTester(
            self, self.GRAPH_ENDPOINT)
        self.tester.setUp()

    def tearDown(self):
        self.tester.tearDown()
        super(TestGraphView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        super(TestGraphView, cls).tearDownClass()

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
        super(TestGanttView, cls).setUpClass()

    def setUp(self):
        super(TestGanttView, self).setUp()
        self.tester = ViewWithDateTimeAndNumRunsAndDagRunsFormTester(
            self, self.GANTT_ENDPOINT)
        self.tester.setUp()

    def tearDown(self):
        self.tester.tearDown()
        super(TestGanttView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        super(TestGanttView, cls).tearDownClass()

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


if __name__ == '__main__':
    unittest.main()
