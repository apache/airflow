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
import copy
import logging.config
import mock
import os
import shutil
import tempfile
import unittest
import sys
import json

from urllib.parse import quote_plus
from werkzeug.test import Client

from airflow import models, configuration
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.settings import Session
from airflow.utils.timezone import datetime
from airflow.www import app as application
from airflow import configuration as conf


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
            'conn_id': 'airflow_db',
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


class TestLogView(unittest.TestCase):
    DAG_ID = 'dag_for_testing_log_view'
    TASK_ID = 'task_for_testing_log_view'
    DEFAULT_DATE = datetime(2017, 9, 1)
    ENDPOINT = '/admin/airflow/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}'.format(
        dag_id=DAG_ID,
        task_id=TASK_ID,
        execution_date=DEFAULT_DATE,
    )

    @classmethod
    def setUpClass(cls):
        super(TestLogView, cls).setUpClass()
        session = Session()
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == cls.DAG_ID and
            TaskInstance.task_id == cls.TASK_ID and
            TaskInstance.execution_date == cls.DEFAULT_DATE).delete()
        session.commit()
        session.close()

    def setUp(self):
        super(TestLogView, self).setUp()

        # Create a custom logging configuration
        configuration.load_test_config()
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        logging_config['handlers']['task']['base_log_folder'] = os.path.normpath(
            os.path.join(current_dir, 'test_logs'))
        logging_config['handlers']['task']['filename_template'] = \
            '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts | replace(":", ".") }}/{{ try_number }}.log'

        # Write the custom logging configuration to a file
        self.settings_folder = tempfile.mkdtemp()
        settings_file = os.path.join(self.settings_folder, "airflow_local_settings.py")
        new_logging_file = "LOGGING_CONFIG = {}".format(logging_config)
        with open(settings_file, 'w') as handle:
            handle.writelines(new_logging_file)
        sys.path.append(self.settings_folder)
        conf.set('core', 'logging_config_class', 'airflow_local_settings.LOGGING_CONFIG')

        app = application.create_app(testing=True)
        self.app = app.test_client()
        self.session = Session()
        from airflow.www.views import dagbag
        dag = DAG(self.DAG_ID, start_date=self.DEFAULT_DATE)
        task = DummyOperator(task_id=self.TASK_ID, dag=dag)
        dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
        ti = TaskInstance(task=task, execution_date=self.DEFAULT_DATE)
        ti.try_number = 1
        self.session.merge(ti)
        self.session.commit()

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        self.session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.DAG_ID and
            TaskInstance.task_id == self.TASK_ID and
            TaskInstance.execution_date == self.DEFAULT_DATE).delete()
        self.session.commit()
        self.session.close()

        sys.path.remove(self.settings_folder)
        shutil.rmtree(self.settings_folder)
        conf.set('core', 'logging_config_class', '')

        super(TestLogView, self).tearDown()

    def test_get_file_task_log(self):
        response = self.app.get(
            TestLogView.ENDPOINT,
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn('Log by attempts',
                      response.data.decode('utf-8'))

    def test_get_logs_with_metadata(self):
        url_template = "/admin/airflow/get_logs_with_metadata?dag_id={}&" \
                       "task_id={}&execution_date={}&" \
                       "try_number={}&metadata={}"
        response = \
            self.app.get(url_template.format(self.DAG_ID,
                                             self.TASK_ID,
                                             quote_plus(self.DEFAULT_DATE.isoformat()),
                                             1,
                                             json.dumps({})))

        self.assertIn('"message":', response.data.decode('utf-8'))
        self.assertIn('"metadata":', response.data.decode('utf-8'))
        self.assertIn('Log for testing.', response.data.decode('utf-8'))
        self.assertEqual(200, response.status_code)

    def test_get_logs_with_null_metadata(self):
        url_template = "/admin/airflow/get_logs_with_metadata?dag_id={}&" \
                       "task_id={}&execution_date={}&" \
                       "try_number={}&metadata=null"
        response = \
            self.app.get(url_template.format(self.DAG_ID,
                                             self.TASK_ID,
                                             quote_plus(self.DEFAULT_DATE.isoformat()),
                                             1))

        self.assertIn('"message":', response.data.decode('utf-8'))
        self.assertIn('"metadata":', response.data.decode('utf-8'))
        self.assertIn('Log for testing.', response.data.decode('utf-8'))
        self.assertEqual(200, response.status_code)


class TestVarImportView(unittest.TestCase):

    IMPORT_ENDPOINT = '/admin/airflow/varimport'

    @classmethod
    def setUpClass(cls):
        super(TestVarImportView, cls).setUpClass()
        session = Session()
        session.query(models.User).delete()
        session.commit()
        user = models.User(username='airflow')
        session.add(user)
        session.commit()
        session.close()

    def setUp(self):
        super(TestVarImportView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()

    def tearDown(self):
        super(TestVarImportView, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        session = Session()
        session.query(models.User).delete()
        session.commit()
        session.close()
        super(TestVarImportView, cls).tearDownClass()

    def test_import_variable_fail(self):
        with mock.patch('airflow.models.Variable.set') as set_mock:
            set_mock.side_effect = UnicodeEncodeError
            content = '{"fail_key": "fail_val"}'

            try:
                # python 3+
                bytes_content = io.BytesIO(bytes(content, encoding='utf-8'))
            except TypeError:
                # python 2.7
                bytes_content = io.BytesIO(bytes(content))
            response = self.app.post(
                self.IMPORT_ENDPOINT,
                data={'file': (bytes_content, 'test.json')},
                follow_redirects=True
            )
            self.assertEqual(response.status_code, 200)
            session = Session()
            db_dict = {x.key: x.get_val() for x in session.query(models.Variable).all()}
            session.close()
            self.assertNotIn('fail_key', db_dict)

    def test_import_variables(self):
        content = ('{"str_key": "str_value", "int_key": 60,'
                   '"list_key": [1, 2], "dict_key": {"k_a": 2, "k_b": 3}}')
        try:
            # python 3+
            bytes_content = io.BytesIO(bytes(content, encoding='utf-8'))
        except TypeError:
            # python 2.7
            bytes_content = io.BytesIO(bytes(content))
        response = self.app.post(
            self.IMPORT_ENDPOINT,
            data={'file': (bytes_content, 'test.json')},
            follow_redirects=True
        )
        self.assertEqual(response.status_code, 200)
        session = Session()
        # Extract values from Variable
        db_dict = {x.key: x.get_val() for x in session.query(models.Variable).all()}
        session.close()
        self.assertIn('str_key', db_dict)
        self.assertIn('int_key', db_dict)
        self.assertIn('list_key', db_dict)
        self.assertIn('dict_key', db_dict)
        self.assertEquals('str_value', db_dict['str_key'])
        self.assertEquals('60', db_dict['int_key'])
        self.assertEquals('[1, 2]', db_dict['list_key'])

        case_a_dict = '{"k_a": 2, "k_b": 3}'
        case_b_dict = '{"k_b": 3, "k_a": 2}'
        try:
            self.assertEquals(case_a_dict, db_dict['dict_key'])
        except AssertionError:
            self.assertEquals(case_b_dict, db_dict['dict_key'])


class TestMountPoint(unittest.TestCase):
    def setUp(self):
        super(TestMountPoint, self).setUp()
        configuration.load_test_config()
        configuration.conf.set("webserver", "base_url", "http://localhost:8080/test")
        config = dict()
        config['WTF_CSRF_METHODS'] = []
        # Clear cached app to remount base_url forcefully
        application.app = None
        app = application.cached_app(config=config, testing=True)
        self.client = Client(app)

    def test_mount(self):
        response, _, _ = self.client.get('/', follow_redirects=True)
        txt = b''.join(response)
        self.assertEqual(b"Apache Airflow is not at this location", txt)

        response, _, _ = self.client.get('/test', follow_redirects=True)
        resp_html = b''.join(response)
        self.assertIn(b"DAGs", resp_html)


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
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
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
        self.session.query(DagRun).filter(
            DagRun.dag_id == self.DAG_ID).delete()
        self.session.commit()
        self.session.close()

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
        Tests graph view with no URL parameter.
        Should show all dag runs in the drop down.
        Should select the latest dag run.
        Should set base date to current date (not asserted)
        """
        response = self.app.get(
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
        Tests graph view with execution_date URL parameter.
        Scenario: click link from dag runs view.
        Should only show dag runs older than execution_date in the drop down.
        Should select the particular dag run.
        Should set base date to execution date.
        """
        response = self.app.get(
            self.endpoint + '&execution_date={}'.format(
                self.runs[1].execution_date.isoformat())
        )
        self.test.assertEqual(response.status_code, 200)
        data = response.data.decode('utf-8')
        self.assertBaseDateAndNumRuns(
            self.runs[1].execution_date,
            configuration.getint('webserver', 'default_dag_run_display_number'),
            data)
        self.assertRunIsNotInDropdown(self.runs[0], data)
        self.assertRunIsSelected(self.runs[1], data)
        self.assertRunIsInDropdownNotSelected(self.runs[2], data)
        self.assertRunIsInDropdownNotSelected(self.runs[3], data)

    def test_with_base_date_and_num_runs_parmeters_only(self):
        """
        Tests graph view with base_date and num_runs URL parameters.
        Should only show dag runs older than base_date in the drop down,
        limited to num_runs.
        Should select the latest dag run.
        Should set base date and num runs to submitted values.
        """
        response = self.app.get(
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
        Tests graph view with base_date and num_runs and execution-date URL parameters.
        Scenario: change the base date and num runs and press "Go",
        the selected execution date is outside the new range.
        Should only show dag runs older than base_date in the drop down.
        Should select the latest dag run within the range.
        Should set base date and num runs to submitted values.
        """
        response = self.app.get(
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
        Tests graph view with base_date and num_runs and execution-date URL parameters.
        Scenario: change the base date and num runs and press "Go",
        the selected execution date is within the new range.
        Should only show dag runs older than base_date in the drop down.
        Should select the dag run with the execution date.
        Should set base date and num runs to submitted values.
        """
        response = self.app.get(
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


class TestGraphView(unittest.TestCase):
    GRAPH_ENDPOINT = '/admin/airflow/graph?dag_id={dag_id}'.format(
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


class TestGanttView(unittest.TestCase):
    GANTT_ENDPOINT = '/admin/airflow/gantt?dag_id={dag_id}'.format(
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


class TestTaskInstanceView(unittest.TestCase):
    TI_ENDPOINT = '/admin/taskinstance/?flt2_execution_date_greater_than={}'

    def setUp(self):
        super(TestTaskInstanceView, self).setUp()
        configuration.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()

    def test_start_date_filter(self):
        resp = self.app.get(self.TI_ENDPOINT.format('2018-10-09+22:44:31'))
        # We aren't checking the logic of the date filter itself (that is built
        # in to flask-admin) but simply that our UTC conversion was run - i.e. it
        # doesn't blow up!
        self.assertEqual(resp.status_code, 200)


class TestDeleteDag(unittest.TestCase):

    def setUp(self):
        conf.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()

    def test_delete_dag_button_normal(self):
        resp = self.app.get('/', follow_redirects=True)
        self.assertIn('/delete?dag_id=example_bash_operator', resp.data.decode('utf-8'))
        self.assertIn("return confirmDeleteDag('example_bash_operator')", resp.data.decode('utf-8'))

    def test_delete_dag_button_for_dag_on_scheduler_only(self):
        # Test for JIRA AIRFLOW-3233 (PR 4069):
        # The delete-dag URL should be generated correctly for DAGs
        # that exist on the scheduler (DB) but not the webserver DagBag

        test_dag_id = "non_existent_dag"

        session = Session()
        DM = models.DagModel
        session.query(DM).filter(DM.dag_id == 'example_bash_operator').update({'dag_id': test_dag_id})
        session.commit()

        resp = self.app.get('/', follow_redirects=True)
        self.assertIn('/delete?dag_id={}'.format(test_dag_id), resp.data.decode('utf-8'))
        self.assertIn("return confirmDeleteDag('{}')".format(test_dag_id), resp.data.decode('utf-8'))

        session.query(DM).filter(DM.dag_id == test_dag_id).update({'dag_id': 'example_bash_operator'})
        session.commit()


class TestTriggerDag(unittest.TestCase):

    def setUp(self):
        conf.load_test_config()
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        self.app = app.test_client()
        self.session = Session()
        models.DagBag().get_dag("example_bash_operator").sync_to_db()

    def test_trigger_dag_button_normal_exist(self):
        resp = self.app.get('/', follow_redirects=True)
        self.assertIn('/trigger?dag_id=example_bash_operator', resp.data.decode('utf-8'))
        self.assertIn("return confirmDeleteDag('example_bash_operator')", resp.data.decode('utf-8'))

    def test_trigger_dag_button(self):

        test_dag_id = "example_bash_operator"

        DR = models.DagRun
        self.session.query(DR).delete()
        self.session.commit()

        self.app.get('/admin/airflow/trigger?dag_id={}'.format(test_dag_id))

        run = self.session.query(DR).filter(DR.dag_id == test_dag_id).first()
        self.assertIsNotNone(run)
        self.assertIn("manual__", run.run_id)


if __name__ == '__main__':
    unittest.main()
